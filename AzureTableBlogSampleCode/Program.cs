using System;

namespace AzureTableBlogSampleCode
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello Azure Storage :) \r\n");
            Console.WriteLine("1) Table - Update 1000 Datas to Storage");
            Console.WriteLine("2) Table - Update Data without ETag");
            Console.WriteLine("3) Table - Read One Data");
            Console.WriteLine("4) Table - Update Data with ETag");
            Console.WriteLine("5) Table - Delete Data");
            Console.WriteLine("6) Table - Read All Datas By PartitionKey");
            Console.WriteLine("e) Exit Test.");
            ShowMenu();
        }

        static void ShowMenu()
        {

            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("");
            Console.Write("Choose Command \\>");

            var res = Console.ReadLine();

            switch (res.Trim())
            {
                case "1":
                    UpdateDatasToStrageTable();
                    break;
                case "2":
                    UpdateOneData();
                    break;
                case "3":
                    ReadOneData();
                    break;
                case "4":
                    UpdateDataWithEtag();
                    break;
                case "5":
                    DeleteData();
                    break;
                case "6":
                    ReadByPK();
                    break;
                case "e":
                    Environment.Exit(-1);
                    break;
                default:
                    Console.WriteLine("\r\nPlease Select Command..\r\n");
                    break;
            }


            ShowMenu();


        }

        private static void ReadByPK()
        {

            var connectionString = "your_connection_string";
            var tableName = "USERSAMPLE";

            var CSAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);
            var CTClient = CSAccount.CreateCloudTableClient();
            var CTable = CTClient.GetTableReference(tableName);

            var query = new Microsoft.WindowsAzure.Storage.Table.TableQuery<User>();
           
            query.FilterString = Microsoft.WindowsAzure.Storage.Table.TableQuery.GenerateFilterCondition("PartitionKey", Microsoft.WindowsAzure.Storage.Table.QueryComparisons.Equal, "GROUP4");

            var res = new System.Collections.Concurrent.ConcurrentBag<User>();

            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();


            Microsoft.WindowsAzure.Storage.Table.TableContinuationToken continuationToken = null;
            do
            {
                var page = CTable.ExecuteQuerySegmentedAsync(query, continuationToken).Result;
                continuationToken = page.ContinuationToken;
                if (page.Results != null)
                {
                    foreach (var obj in page.Results)
                    {
                        res.Add(obj);
                    }
                }
            }
            while (continuationToken != null);

            stopwatch.Stop();
            Console.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(res));

            Console.WriteLine("Count >>" + res.Count);


            Console.WriteLine(stopwatch.Elapsed.ToString());

        }

        private static void DeleteData()
        {
            var connectionString = "your_connection_string";
            var tableName = "USERSAMPLE";

            var CSAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);
            var CTClient = CSAccount.CreateCloudTableClient();
            var CTable = CTClient.GetTableReference(tableName);

            //確保沒有該張 Table 就建立
            var resCreate = CTable.CreateIfNotExistsAsync().Result;

            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();



            Microsoft.WindowsAzure.Storage.Table.TableOperation operation = Microsoft.WindowsAzure.Storage.Table.TableOperation.Delete(
                new User { RowKey = "USER333", PartitionKey = "GROUP4", ETag = "*" });

            var res = CTable.ExecuteAsync(operation).Result;

            Console.WriteLine("Success Delete");

            stopwatch.Stop();

            Console.WriteLine(stopwatch.Elapsed.ToString());
        }


        private static void UpdateDataWithEtag()
        {
            var connectionString = "your_connection_string";
            var tableName = "USERSAMPLE";

            var CSAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);
            var CTClient = CSAccount.CreateCloudTableClient();
            var CTable = CTClient.GetTableReference(tableName);

            //確保沒有該張 Table 就建立
            var resCreate = CTable.CreateIfNotExistsAsync().Result;

            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();



            Microsoft.WindowsAzure.Storage.Table.TableOperation operation = Microsoft.WindowsAzure.Storage.Table.TableOperation.Retrieve<User>("GROUP10", "USER999");
            var user = CTable.ExecuteAsync(operation).Result.Result as User;
            if (user != null)
            {

                Console.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(user));

                user.Name = "使用者" + 999 + "--更改過了第二次!!!";

                var operationUpdate = Microsoft.WindowsAzure.Storage.Table.TableOperation.Replace(user);
                var res = CTable.ExecuteAsync(operationUpdate).Result;

                Console.WriteLine("Success");
            }
            else
            {
                Console.WriteLine("NODATA!");
            }

            stopwatch.Stop();

            Console.WriteLine(stopwatch.Elapsed.ToString());
        }


        private static void ReadOneData() {

            var connectionString = "your_connection_string";
            var tableName = "USERSAMPLE";

            var CSAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);
            var CTClient = CSAccount.CreateCloudTableClient();
            var CTable = CTClient.GetTableReference(tableName);

            Microsoft.WindowsAzure.Storage.Table.TableOperation operation = Microsoft.WindowsAzure.Storage.Table.TableOperation.Retrieve<User>("GROUP10", "USER999");
            var res = CTable.ExecuteAsync(operation).Result.Result;
            if (res != null)
            {

                Console.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(res));
            }
            else {
                Console.WriteLine("NODATA!");
            }
        }


        private static void UpdateOneData() {
            var connectionString = "your_connection_string";
            var tableName = "USERSAMPLE";

            var CSAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);
            var CTClient = CSAccount.CreateCloudTableClient();
            var CTable = CTClient.GetTableReference(tableName);

            //確保沒有該張 Table 就建立
            var resCreate = CTable.CreateIfNotExistsAsync().Result;

            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            var user = new User();
            user.Birth = DateTime.Now.AddHours(999);
            user.Mobile = (999 + 9752000).ToString();
            user.Salary = 999 * 1000;
            user.Name = "使用者" + 999+"--更改過了";
            user.RowKey = "USER" + 999;
            user.PartitionKey = "GROUP" + (Convert.ToInt16(Math.Ceiling((decimal)999 / 100)));
            //Etag must be *
            user.ETag = "*";
            var operation = Microsoft.WindowsAzure.Storage.Table.TableOperation.Replace(user);
            var res = CTable.ExecuteAsync(operation).Result;

            stopwatch.Stop();

            Console.WriteLine(stopwatch.Elapsed.ToString());
        }

        private static void UpdateDatasToStrageTable()
        {

            var connectionString = "your_connection_string";
            var tableName = "USERSAMPLE";

            var CSAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(connectionString);
            var CTClient = CSAccount.CreateCloudTableClient();
            var CTable = CTClient.GetTableReference(tableName);

            //確保沒有該張 Table 就建立
            var resCreate = CTable.CreateIfNotExistsAsync().Result;

            System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
            stopwatch.Start();

            System.Threading.Tasks.Parallel.For(1, 1001, i =>
            {

                var user = new User();
                user.Birth = DateTime.Now.AddHours(i);
                user.Mobile = (i + 9752000).ToString();
                user.Salary = i * 1000;
                user.Name = "使用者" + i;
                user.RowKey = "USER" + i;
                user.PartitionKey = "GROUP" + (Convert.ToInt16(Math.Ceiling((decimal)i / 100)));
              
                var operation = Microsoft.WindowsAzure.Storage.Table.TableOperation.InsertOrReplace(user);
                var res = CTable.ExecuteAsync(operation).Result;
            }); 

            stopwatch.Stop();

            Console.WriteLine(stopwatch.Elapsed.ToString());

        }
    }
}
