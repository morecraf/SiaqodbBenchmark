using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Connection;
using Sqo;
using Sqo.Documents;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Siaqodb_vs_RavenDB
{

    class Program
    {
        private const int ENTITY_COUNT = 10000;

        private const string siaqodbPath = @"F:\siaqodb";
        private const string ravenDBPath = @"F:\siaqodb\raven\";

        private const int OneMB = 1024 * 1024;

        static List<MyEntity> entities = new List<MyEntity>(ENTITY_COUNT);

        static void Main(string[] args)
        {
            //set trial license
            SiaqodbConfigurator.SetLicense(@"5+Qd+DpF8OEzVWEnEPvYAqMJxL7iMR9d48njC6pSTNM=");
            SiaqodbConfigurator.SetDocumentSerializer(new MyJsonSerializer());


            Insert();
            ReadAll();
            ReadByPrimaryIndex();
            ReadBySecondaryIndex();
            Update();
            Delete();

            Console.WriteLine("Press enter...");
            Console.ReadLine();


        }
        public static void Insert()
        {
            GenerateEntities();

            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 300 * OneMB, 200);

                Console.WriteLine("Siaqodb INSERT...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var trans = siaqodb.BeginTransaction();
                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    Document doc = new Document();
                    doc.Key = entities[i].Id;
                    //set document content
                    doc.SetContent<MyEntity>(entities[i]);
                    //set tags(indexes)
                    doc.SetTag("myint", entities[i].IntValue);

                    //store the doc within the Bucket called 'myentities'
                    siaqodb.Documents["myentities"].Store(doc, trans);
                }
                trans.Commit();
                stopwatch.Stop();
                Console.WriteLine("Siaqodb INSERT took:" + stopwatch.Elapsed);

            }
            using (EmbeddableDocumentStore store = new EmbeddableDocumentStore
            {
                DataDirectory = ravenDBPath

            })
            {
                store.Configuration.DefaultStorageTypeName = "voron";
                store.Initialize(); // initializes document store, by connecting to server and downloading various configurations

                store.DatabaseCommands.PutIndex("MyEntity/IntValue",
                      new IndexDefinitionBuilder<MyEntity>()
                      {
                          Map = myints => from mi in myints
                                          select new { IntValue = mi.IntValue }
                      }, true);

                using (BulkInsertOperation bulkInsert = store.BulkInsert())
                {
                    Console.WriteLine("RavenDB INSERT...");
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();

                    for (int i = 0; i < ENTITY_COUNT; i++)
                    {
                        bulkInsert.Store(entities[i], entities[i].Id);

                    }

                    stopwatch.Stop();
                    Console.WriteLine("RavenDB INSERT took:" + stopwatch.Elapsed);



                }
                using (IDocumentSession session = store.OpenSession()) // opens a session that will work in context of 'DefaultDatabase'
                {
                    //waiting for index to finish write operations
                    int temp = entities[0].IntValue;
                    var qq = (from myentity in session.Query<MyEntity>("MyEntity/IntValue").Customize(a => a.WaitForNonStaleResults())
                              where myentity.IntValue == temp
                              select myentity).ToList();
                }

            }

        }
       

    
        public static void ReadAll()
        {
            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 200 * OneMB, 20);
                Console.WriteLine("Siaqodb READ ALL...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                int start = 0;
                List<Document> docs = new List<Document>();
                while (true)
                {
                    var current = siaqodb.Documents["myentities"].LoadAll(start, 1024);
                    if (current.Count == 0)
                        break;

                    start += current.Count;
                    docs.AddRange(current);

                }
                foreach (Document d in docs)
                {
                    var entity = d.GetContent<MyEntity>();
                }

                stopwatch.Stop();
                Console.WriteLine("Siaqodb READ ALL (" + docs.Count + " items) took:" + stopwatch.Elapsed);

            }
            using (EmbeddableDocumentStore store = new EmbeddableDocumentStore
            {
                DataDirectory = ravenDBPath

            })
            {
                store.Configuration.DefaultStorageTypeName = "voron";
                store.Initialize(); // initializes document store, by connecting to server and downloading various configurations

                Console.WriteLine("RavenDB READ ALL...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                int start = 0;
                List<MyEntity> docs = new List<MyEntity>();
                while (true)
                {

                    using (IDocumentSession session = store.OpenSession()) // opens a session that will work in context of 'DefaultDatabase'
                    {
                        bool finished = false;
                        for (int i = 0; i < 30; i++)//raven allows max 30 ops per session
                        {
                            var current = session.Query<MyEntity>().Take(1024).Skip(start).ToList();
                            if (current.Count == 0)
                            {
                                finished = true;
                                break;
                            }

                            start += current.Count;
                            docs.AddRange(current);
                        }
                        if (finished)
                            break;
                    }

                }

                stopwatch.Stop();
                Console.WriteLine("RavenDB READ ALL(" + docs.Count + " items) took:" + stopwatch.Elapsed);

            }


        }
        public static void ReadByPrimaryIndex()
        {
            
            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 100 * OneMB, 20);
                Console.WriteLine("Siaqodb READ_BY_PK...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                List<MyEntity> loadedENs = new List<MyEntity>(ENTITY_COUNT);
                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    var en = siaqodb.Documents["myentities"].Load<MyEntity>(entities[i].Id);
                    loadedENs.Add(en);
                }

                stopwatch.Stop();
                Console.WriteLine("Siaqodb READ_BY_PK took:" + stopwatch.Elapsed);

            }
            using (EmbeddableDocumentStore store = new EmbeddableDocumentStore
            {
                DataDirectory = ravenDBPath

            })
            {
                store.Configuration.DefaultStorageTypeName = "voron";
                store.Initialize(); // initializes document store, by connecting to server and downloading various configurations
                Console.WriteLine("RavenDB READ_BY_PK...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                List<MyEntity> loadedENs = new List<MyEntity>(ENTITY_COUNT);
                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    var jsonDocument = store.DatabaseCommands.Get(entities[i].Id);
                    var en = jsonDocument.DataAsJson.Deserialize<MyEntity>(store.Conventions);
                    loadedENs.Add(en);
                }


                stopwatch.Stop();
                Console.WriteLine("RavenDB READ_BY_PK took:" + stopwatch.Elapsed);

            }


        }
        public static void ReadBySecondaryIndex()
        {
           
            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 100 * OneMB, 20);
                Console.WriteLine("Siaqodb READ_BY_SECONDARY_INDEX...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    var docs = (from Document doc in siaqodb.Documents["myentities"]
                                where doc.GetTag<int>("myint") == entities[i].IntValue
                                select doc).ToObjects<MyEntity>();
                  
                }
               
                stopwatch.Stop();
                Console.WriteLine("Siaqodb READ_BY_SECONDARY_INDEX took:" + stopwatch.Elapsed);

            }
            using (EmbeddableDocumentStore store = new EmbeddableDocumentStore
            {
                DataDirectory = ravenDBPath

            })
            {
                store.Configuration.DefaultStorageTypeName = "voron";
                store.Initialize(); // initializes document store, by connecting to server and downloading various configurations
                Console.WriteLine("RavenDB READ_BY_SECONDARY_INDEX...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < ENTITY_COUNT - 30; i += 30)
                {
                    using (IDocumentSession session = store.OpenSession())
                    {
                        for (int j = 0; j < 30; j++)
                        {

                            var temp = entities[i+j].IntValue;
                            var qq = (from myentity in session.Query<MyEntity>("MyEntity/IntValue")
                                      where myentity.IntValue == temp
                                      select myentity).ToList();

                        }
                    }

                }
                stopwatch.Stop();
                Console.WriteLine("RavenDB READ_BY_SECONDARY_INDEX took:" + stopwatch.Elapsed);

            }


        }
        public static void Update()
        {

            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 300 * OneMB, 200);

                Console.WriteLine("Siaqodb UPDATE...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var trans = siaqodb.BeginTransaction();
                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    Document doc = new Document();
                    doc.Key = entities[i].Id;
                    entities[i].StringValue += i.ToString();
                    entities[i].IntValue++;
                    //set document content
                    doc.SetContent<MyEntity>(entities[i]);
                   //update the index too
                    doc.SetTag("myint", entities[i].IntValue);

                    //store the doc within the Bucket called 'myentities'
                    siaqodb.Documents["myentities"].Store(doc, trans);
                }
                trans.Commit();

                stopwatch.Stop();
                Console.WriteLine("Siaqodb UPDATE took:" + stopwatch.Elapsed);

            }
            using (EmbeddableDocumentStore store = new EmbeddableDocumentStore
            {
                DataDirectory = ravenDBPath

            })
            {
                store.Configuration.DefaultStorageTypeName = "voron";
                store.Initialize(); // initializes document store, by connecting to server and downloading various configurations
                Raven.Abstractions.Data.BulkInsertOptions opt = new Raven.Abstractions.Data.BulkInsertOptions()
                { OverwriteExisting = true };
                using (BulkInsertOperation bulkInsert = store.BulkInsert(null,opt ))
                {
                    Console.WriteLine("RavenDB UPDATE...");
                    var stopwatch = new Stopwatch();
                    stopwatch.Start();

                    for (int i = 0; i < ENTITY_COUNT; i++)
                    {
                        entities[i].StringValue += i.ToString();
                        entities[i].IntValue++;
                        bulkInsert.Store(entities[i], entities[i].Id);
                    }
                  

                    stopwatch.Stop();
                    Console.WriteLine("RavenDB UPDATE took:" + stopwatch.Elapsed);

                }
            }

        }
        public static void Delete()
        {

            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 300 * OneMB, 200);

                Console.WriteLine("Siaqodb DELETE...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                var trans = siaqodb.BeginTransaction();
                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    var doc= siaqodb.Documents["myentities"].Load(entities[i].Id);
                    siaqodb.Documents["myentities"].Delete(doc,trans);
                }
                trans.Commit();

                stopwatch.Stop();
                Console.WriteLine("Siaqodb DELETE took:" + stopwatch.Elapsed);

            }
            using (EmbeddableDocumentStore store = new EmbeddableDocumentStore
            {
                DataDirectory = ravenDBPath

            })
            {
                store.Configuration.DefaultStorageTypeName = "voron";
                store.Initialize(); // initializes document store, by connecting to server and downloading various configurations


                Console.WriteLine("RavenDB DELETE...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < ENTITY_COUNT - 30; i += 30)
                {
                    using (IDocumentSession session = store.OpenSession())
                    {
                        for (int j = 0; j < 30; j++)
                        {
                            session.Delete(entities[i].Id);
                        }
                        session.SaveChanges();
                    }
                }
               

                stopwatch.Stop();
                Console.WriteLine("RavenDB DELETE took:" + stopwatch.Elapsed);

            }

        }


        public static void GenerateEntities()
        {
            var random = new Random(DateTime.Now.Millisecond);

            for (int i = 0; i < ENTITY_COUNT; i++)
            {

                entities.Add(new MyEntity
                {
                    Id = Guid.NewGuid().ToString(),
                    IntValue = random.Next(),
                    DoubleValue = random.NextDouble(),
                    StringValue = Guid.NewGuid().ToString(),
                    DateTimeValue = new DateTime(random.Next(1999, 2015), random.Next(1, 12), random.Next(1, 28)),
                    GuidValue = Guid.NewGuid()
                }
                );
            }
        }
    }
    public class MyEntity
    {
        public string Id { get; set; }
        public int IntValue { get; set; }
        public string StringValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
        public double DoubleValue { get; set; }
    }
    internal class MyJsonSerializer : IDocumentSerializer
    {

        public object Deserialize(Type type, byte[] objectBytes)
        {
            string jsonStr = Encoding.UTF8.GetString(objectBytes);
            return JsonConvert.DeserializeObject(jsonStr, type);

        }

        public byte[] Serialize(object obj)
        {
            string jsonStr = JsonConvert.SerializeObject(obj, Formatting.Indented);
            return Encoding.UTF8.GetBytes(jsonStr);
        }

    }

}
