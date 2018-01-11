using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace DemoTableStorageCore
{
    class Program
    {
        //docs.microsoft.com/pt-br/azure/visual-studio/vs-storage-aspnet5-getting-started-tables

        static CloudTable table;

        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();
            var storageConn = configuration.GetSection("Keys").GetSection("StorageAccount").Value;
            var tableName = configuration.GetSection("Keys").GetSection("TableStorageName").Value;
            //Console.WriteLine(storageConn);

            // Parse the connection string and return a reference to the storage account.
            var storageAccount = CloudStorageAccount.Parse(storageConn);
            
            
            // Create the table client.
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            // Retrieve a reference to the table.
            table = tableClient.GetTableReference(tableName);

            // Create the table if it doesn't exist.
            table.CreateIfNotExistsAsync().Wait();


            //AddNewCustomer();
            //AddBatchOperation();

            GetAll();
            //GetByPartition();
            //GetOne();
            //DeleteEntity();

            Console.WriteLine("Fim");
            Console.ReadLine();
        }

        private static void GetAll()
        {
            Console.WriteLine("Obter todas as entidades");

            var query = new TableQuery<CustomerEntity>().Take(2);

            // Print the fields for each customer.
            TableContinuationToken token = null;
            do
            {
                TableQuerySegment<CustomerEntity> resultSegment = table.ExecuteQuerySegmentedAsync(query, token).Result;
                token = resultSegment.ContinuationToken;

                foreach (CustomerEntity entity in resultSegment.Results)
                {
                    Console.WriteLine("{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.PhoneNumber);
                }
            } while (token != null);
        }

        private static void DeleteEntity()
        {
            Console.WriteLine("Excluir uma entidade");

            // Create a retrieve operation that expects a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith 3", "Ben 0");

            // Execute the operation.
            TableResult retrievedResult = table.ExecuteAsync(retrieveOperation).Result;

            // Assign the result to a CustomerEntity object.
            CustomerEntity deleteEntity = (CustomerEntity)retrievedResult.Result;

            // Create the Delete TableOperation and then execute it.
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                table.ExecuteAsync(deleteOperation).Wait();

                Console.WriteLine("Entity deleted.");
            }

            else
                Console.WriteLine("Couldn't delete the entity.");
        }

        private static void GetOne()
        {
            Console.WriteLine("Obter uma única entidade");

            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<CustomerEntity>("Smith 3", "Ben 0");

            // Execute the retrieve operation.
            TableResult retrievedResult = table.ExecuteAsync(retrieveOperation).Result;

            // Print the phone number of the result.
            if (retrievedResult.Result != null)
                Console.WriteLine(((CustomerEntity)retrievedResult.Result).PhoneNumber);
            else
                Console.WriteLine("The phone number could not be retrieved.");
        }

        private static void GetByPartition()
        {
            Console.WriteLine("Obter todas as entidades em uma partição");
            // Construct the query operation for all customer entities where PartitionKey="Smith".
            TableQuery<CustomerEntity> query = new TableQuery<CustomerEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Smith 3"));

            // Print the fields for each customer.
            TableContinuationToken token = null;
            do
            {
                TableQuerySegment<CustomerEntity> resultSegment = table.ExecuteQuerySegmentedAsync(query, token).Result;
                token = resultSegment.ContinuationToken;

                foreach (CustomerEntity entity in resultSegment.Results)
                {
                    Console.WriteLine("{0}, {1}\t{2}\t{3}", entity.PartitionKey, entity.RowKey,
                    entity.Email, entity.PhoneNumber);
                }
            } while (token != null);
        }

        private static void AddBatchOperation()
        {
            // Create the batch operation.
            TableBatchOperation batchOperation = new TableBatchOperation();

            // Create a customer entity and add it to the table.
            CustomerEntity customer1 = new CustomerEntity($"Smith {GetRandom()}", $"Jeff {GetRandom()}");
            customer1.Email = "Jeff@contoso.com";
            customer1.PhoneNumber = "425-555-0104";

            // Create another customer entity and add it to the table.
            CustomerEntity customer2 = new CustomerEntity($"Smith {GetRandom()}", $"Ben {GetRandom()}");
            customer2.Email = "Ben@contoso.com";
            customer2.PhoneNumber = "425-555-0102";

            // Add both customer entities to the batch insert operation.
            batchOperation.Insert(customer1);
            batchOperation.Insert(customer2);

            // Execute the batch operation.
            table.ExecuteBatchAsync(batchOperation).Wait();
            Console.WriteLine("Adicionado customers em batch");
        }

        private static void AddNewCustomer()
        {
            // Create a new customer entity.
            CustomerEntity customer1 = new CustomerEntity($"Harp {GetRandom()}", $"Walter {GetRandom()}");
            customer1.Email = "Walter@contoso.com";
            customer1.PhoneNumber = "425-555-0101";

            // Create the TableOperation that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(customer1);

            // Execute the insert operation.
            table.ExecuteAsync(insertOperation).Wait();

            Console.WriteLine("Adicionado customer");
        }

        private static int GetRandom()
        {
            return new Random().Next(0, 5);
        }
    }
    
    public class CustomerEntity : TableEntity
    {
        public CustomerEntity(string lastName, string firstName)
        {
            this.PartitionKey = lastName;
            this.RowKey = firstName;
        }

        public CustomerEntity() { }

        public string Email { get; set; }

        public string PhoneNumber { get; set; }
    }

}
