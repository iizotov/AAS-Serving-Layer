using System;
using Microsoft.AnalysisServices;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AnalysisServices.Tabular.DataRefresh;
using System.Collections.Generic;

namespace AAS_Serving_Layer
{
    class Program
    {
        //AAS
        static string AASUser = "<CHANGEME>";
        static string AASPassword = "<CHANGEME>";
        static string AASServer = "asazure://southeastasia.asazure.windows.net/CHANGEME";
        static string AASConnectionString = String.Format("Provider=MSOLAP;Data Source={0};User ID={1};Password={2};Persist Security Info=True;Impersonation Level=Impersonate", AASServer, AASUser, AASPassword);
        static string AASDBName = "TESTDB";
        static string AASModelName = "TestTabularModel";
        static string AASTableName = "Sales";
        static string AASPushPartitionName = "PushPartition";
        static string AASAppendPartitionName = "AppendPartition";
        static string AASTempPartitionName = "TempPartition";


        //SQL - feel free to use my instance
        static string SQLUser = "ROUser";
        static string SQLPassword = "P@ssw0rd";
        static string SQLDatabase = "AdventureWorks2014";
        static string SQLServer = "welcomeall.database.windows.net";

        static int SQLQueryBigRows = 30000;
        static int SQLQuerySmallRows = 300;

        static string SQLQueryNil = @"
            SELECT 
                 [SalesOrderID]
                ,[OrderDate]
                ,[CustomerID]
                ,[TotalDue]
            FROM [SalesLT].[SalesOrderHeader]
            WHERE 1 = 0";

        static string SQLQueryBig = @"
            SELECT TOP 30000
                 NEWID() as [SalesOrderID]
                ,GETDATE() - 1000 * RAND(convert(varbinary, newid())) as [OrderDate]
                ,1.0 + floor(30118 * RAND(convert(varbinary, newid()))) as [CustomerID]
                ,round([TotalDue] * (0.2 + 0.8 * RAND(convert(varbinary, newid()))), 2) as [TotalDue]
            FROM [SalesLT].[SalesOrderHeader]
            CROSS JOIN
            (
	            SELECT TOP 100
		            *
	            FROM
		            sysobjects
            ) [BigTable]
            CROSS JOIN
            (
	            SELECT TOP 10
		            *
	            FROM
		            sysobjects
            ) [BigTable2]";

        static string SQLQuerySmall = @"
           SELECT TOP 300
                 NEWID() as [SalesOrderID]
                ,GETDATE() - 1000 * RAND(convert(varbinary, newid())) as [OrderDate]
                ,1.0 + floor(30118 * RAND(convert(varbinary, newid()))) as [CustomerID]
                ,round([TotalDue] * (0.2 + 0.8 * RAND(convert(varbinary, newid()))), 2) as [TotalDue]
            FROM [SalesLT].[SalesOrderHeader]
            CROSS JOIN
            (
	            SELECT TOP 10
		            *
	            FROM
		            sysobjects
            ) [BigTable]
            CROSS JOIN
            (
	            SELECT TOP 1
		            *
	            FROM
		            sysobjects
            ) [BigTable2]";

        static string AddPartition(Database db, string tableName, string dataSource, string query, string partitionName)
        {

            db.Model.Tables[tableName].Partitions.Add(new Partition()
            {
                Name = partitionName,
                Source = new QueryPartitionSource()
                {
                    DataSource = db.Model.DataSources[dataSource],
                    Query = query,
                },
            });
 
            return partitionName;
        }
        static string AddPushPartition(Database db, string tableName, string partitionName)
        {
            db.Model.Tables[tableName].Partitions.Add(new Partition()
            {
                Name = partitionName,
            });
            return partitionName;
        }

        static void AppendToPartition(Database database, string table, string partitionName, string query)
        {
            Partition partition = database.Model.Tables[table].Partitions[partitionName];
            OverrideCollection oc = new OverrideCollection
            {
                Partitions = {
                    new PartitionOverride {
                        OriginalObject = partition,
                        Source = new QueryPartitionSourceOverride {
                            DataSource = ((QueryPartitionSource)partition.Source).DataSource,
                            Query = query
                        }
                    }
                }
            };
            var listOc = new List<OverrideCollection>();
            listOc.Add(oc);
            partition.RequestRefresh(Microsoft.AnalysisServices.Tabular.RefreshType.Add, listOc);
            database.Update(UpdateOptions.ExpandFull);
        }

        static void AppendToPartitionViaMerge(Database database, string table, string partitionName, string query, string tempPartitionName)
        {
            AddPartition(database, AASTableName, SQLDatabase, SQLQueryNil, AASTempPartitionName);
            database.Update(UpdateOptions.ExpandFull);

            Partition tempPartition = database.Model.Tables[table].Partitions[tempPartitionName];
            Partition appendPartition = database.Model.Tables[table].Partitions[partitionName];
            OverrideCollection oc = new OverrideCollection
            {
                Partitions = {
                    new PartitionOverride {
                        OriginalObject = tempPartition,
                        Source = new QueryPartitionSourceOverride {
                            DataSource = ((QueryPartitionSource)tempPartition.Source).DataSource,
                            Query = query
                        }
                    }
                }
            };
            var listOc = new List<OverrideCollection>();
            listOc.Add(oc);
            tempPartition.RequestRefresh(Microsoft.AnalysisServices.Tabular.RefreshType.Automatic, listOc);
            //database.Update(UpdateOptions.ExpandFull);

            appendPartition.RequestMerge(new List<Partition>() { tempPartition });
            database.Update(UpdateOptions.ExpandFull);

        }

        static void Main(string[] args)
        {
            using (Server server = new Server())
            {
                Console.WriteLine("Press Ctrl+C to stop");
                Console.WriteLine();
                Console.WriteLine("Connecting to {0}", AASServer);
                server.Connect(AASConnectionString);
                Console.WriteLine("Connected");

                Database aasDatabase;

                DateTime start, finish;


                if (!server.Databases.Contains(AASDBName))
                {
                    aasDatabase = new Database()
                    {
                        Name = AASDBName,
                        ID = AASDBName,
                        CompatibilityLevel = 1400,
                        StorageEngineUsed = StorageEngineUsed.TabularMetadata,
                    };
                    Console.WriteLine("Created Database, compat 1400 {0}", AASDBName);

                    aasDatabase.Model = new Model()
                    {
                        Name = AASModelName,
                        Description = AASModelName
                    };
                    Console.WriteLine("Created Model {0}", AASModelName);


                    aasDatabase.Model.DataSources.Add(new ProviderDataSource()
                    {
                        Name = SQLDatabase,
                        Description = "A sample SQL DataSource.",
                        ConnectionString = String.Format("Provider=SQLNCLI11;Server=tcp:{0};Initial Catalog={1};Persist Security Info=False;User ID={2};Password={3};MultipleActiveResultSets=True;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30", SQLServer, SQLDatabase, SQLUser, SQLPassword),
                        ImpersonationMode = Microsoft.AnalysisServices.Tabular.ImpersonationMode.ImpersonateAccount,
                        Account = SQLUser,
                        Password = SQLPassword,
                    });
                    Console.WriteLine("Registered SQL DB Datasource {0}", SQLDatabase);

                    aasDatabase.Model.Tables.Add(new Table()
                    {
                        Name = aasDatabase.Model.Tables.GetNewName(AASTableName),
                        Description = AASTableName,
                        Columns =
                        {
                            new DataColumn() {
                                Name = "SalesOrderID",
                                DataType = DataType.String,
                                SourceColumn = "SalesOrderID",
                            },
                            new DataColumn() {
                                Name = "OrderDate",
                                DataType = DataType.DateTime,
                                SourceColumn = "OrderDate",
                            },
                            new DataColumn() {
                                Name = "CustomerID",
                                DataType = DataType.Int64,
                                SourceColumn = "CustomerID",
                            },
                            new DataColumn() {
                                Name = "TotalDue",
                                DataType = DataType.Decimal,
                                SourceColumn = "TotalDue",
                            },
                        }
                    });
                    Console.WriteLine("Created Table {0}", AASTableName);
                    AddPushPartition(aasDatabase, AASTableName, AASPushPartitionName);
                    Console.WriteLine("Adding push partition {0}", AASPushPartitionName);
                    AddPartition(aasDatabase, AASTableName, SQLDatabase, SQLQueryNil, AASAppendPartitionName);
                    Console.WriteLine("Adding append partition {0}", AASAppendPartitionName);

                    server.Databases.Add(aasDatabase);
                    aasDatabase.Update(UpdateOptions.ExpandFull);
                }
                else
                {
                    //TODO: add checks for Model, Table, Source
                    aasDatabase = server.Databases.GetByName(AASDBName);
                    Console.WriteLine("Database {0} exists", AASDBName);
                }

                while (true)
                {
                    for (int i = 0; i < 10; i++)
                    {
                        string partitionName = String.Format("{0}-{1}", Guid.NewGuid(), DateTime.Now.Ticks);
                        AddPartition(aasDatabase, AASTableName, SQLDatabase, SQLQueryBig, partitionName);
                        Console.WriteLine("Adding partition {0}", partitionName);
                        aasDatabase.Model.Tables[AASTableName].Partitions[partitionName].RequestRefresh(Microsoft.AnalysisServices.Tabular.RefreshType.Automatic);
                    }

                    start = DateTime.Now;
                    Console.WriteLine("Refreshing Tabular Model {0}", AASModelName);
                    aasDatabase.Update(UpdateOptions.ExpandFull);
                    finish = DateTime.Now;

                    Console.WriteLine("Refresh completed in {0} msec, speed: {1} rows/s, total rows: {2}", 
                        (finish - start).TotalMilliseconds,
                        10 * SQLQueryBigRows / (finish - start).TotalSeconds,
                        SQLQueryBigRows * 10);
                    Console.WriteLine();

                    Console.WriteLine("Appending to {0} 10 times...", AASAppendPartitionName);

                    for (int i = 0; i < 10; i++)
                    {
                        start = DateTime.Now;
                        AppendToPartition(aasDatabase, AASTableName, AASAppendPartitionName, SQLQuerySmall);
                        //AppendToPartitionViaMerge(aasDatabase, AASTableName, AASAppendPartitionName, SQLQuerySmall, AASTempPartitionName);
                        finish = DateTime.Now;
                        Console.WriteLine("{0}/9. Took {1} msec, finished appending to {2}, speed: {3} rows/s, total rows: {4}", 
                            i, 
                            (finish - start).TotalMilliseconds, 
                            AASAppendPartitionName,
                            SQLQuerySmallRows / (finish - start).TotalSeconds,
                            SQLQuerySmallRows);
                    }
                    Console.WriteLine();
                }
            }
        }
    }
}
