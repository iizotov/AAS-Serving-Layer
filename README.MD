# Using AAS as the Serving Layer for your Lambda Architecture
This code sample demonstrates how a Tabular SSAS/AAS instance can be used as the Serving Layer of your [Lambda Architecture](http://lambda-architecture.net/), surfacing the Batch and Speed layers via partitions. The Speed Layer data will be incrementally loaded to a 'hot' partition (or sharded across a number of hot partitions) in small batches and frequently, thus reducing the overall latency. The Batch layer,consisting of historic data, can be loaded on-demand as historic partitions. Hot partitions should be merged or re-loaded via the batch layer as they age.

The demo uses the AdventureWorks database hosted on my publicly available Azure SQL DB instance - feel free to access it using:
``` 
host: welcomeall.database.windows.net
DB: AdventureWorks2014
user: ROUser
password: P@ssw0rd
```

The code demonstrates how to create new partitions and continually append data to the 'hot' partition. It can be potentially refined to [push](https://docs.microsoft.com/en-us/sql/analysis-services/tabular-model-programming-compatibility-level-1200/create-tables-partitions-and-columns-in-a-tabular-model#partitions-in-a-table) data into a partition to reduce the overall latency even further.

**Software prerequisites:**

1. Visual Studio 2015 (or higher) with the latest [AMO Client Libraries installed](https://docs.microsoft.com/en-us/azure/analysis-services/analysis-services-data-providers).
1. SSAS Tabular running locally or permission to create an Azure Analysis Services instance.

## Run this sample
1. Clone this repository or download the zip file.

2. From Visual Studio, open the **AAS-Serving-Layer.sln** file from the root directory.

5. Modify the **/AAS-Serving-Layer/Program.cs** constants using your SSAS/AAS instance
```
static string AASUser = ...
static string AASPassword = ...
static string AASServer = ...
```
4. In Visual Studio Build menu, select **Build Solution** (or Press F6) annd run it by pressing F5

## Disclaimers
The code included in this sample is not intended to be a set of best practices on how to build scalable enterprise grade applications. This is beyond the scope of this quick start sample.

## Related Links
For more information, see these articles:
- [Whitepaper on automated partition management](https://github.com/Microsoft/Analysis-Services/blob/master/AsPartitionProcessing/Automated%20Partition%20Management%20for%20Analysis%20Services%20Tabular%20Models.pdf)
- [Using Process Add in tabular models](https://www.sqlbi.com/articles/using-process-add-in-tabular-models/)

## Alternative Approach Using SQL Server/SQL DB In-memory
- [Ingesting 1.4 million sustained rows per second with In-Memory OLTP & Columnstore Index](https://blogs.msdn.microsoft.com/sqlserverstorageengine/2017/11/08/high-scale-and-performance-of-azure-sql-database/)
- [In-Memory OLTP Common Design Pattern – High Data Input Rate/Shock Absorber](https://blogs.technet.microsoft.com/dataplatforminsider/2013/09/19/in-memory-oltp-common-design-pattern-high-data-input-rateshock-absorber/)