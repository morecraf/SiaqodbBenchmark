using Sqo;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SiaqodbVsSQLite
{
    class Program
    {
        private const int ENTITY_COUNT = 10000;

        private const string siaqodbPath=@"D:\DB\";
        private const string sqLitePath=@"D:\DB\db.sqlite";

        private const int OneMB=1024*1024;   
        static void Main(string[] args)
        {
            //set trial license
            SiaqodbConfigurator.SetLicense("9+3kflAazhBu3bW+lP/eJZR91W03jgYPxZpa9fDHSbk6UNwzo/AjI3hjA161Oqry");

            Insert();
            Update();
            Read();
            Delete();

            Console.WriteLine("Press enter...");
            Console.ReadLine();

           
        }
        public static void Insert()
        {
            var entities = GetEntities().ToArray();
            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 100 * OneMB, 20);

                Console.WriteLine("InsertSiaqodb...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    siaqodb.StoreObject(entities[i]);
                }
              
                stopwatch.Stop();
                Console.WriteLine("InsertSiaqodb took:" + stopwatch.Elapsed);
               
            }

            using (var dbsql = new SQLite.SQLiteConnection(sqLitePath))
            {
                dbsql.CreateTable<MyEntity>();
                Console.WriteLine("InsertSQLite...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < ENTITY_COUNT; i++)
                {
                    dbsql.Insert(entities[i]);
                }

                stopwatch.Stop();
                Console.WriteLine("InsertSQLite took:" + stopwatch.Elapsed);
            }
        }
        public static void Update()
        {
            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 100 * OneMB, 20);
                var all = siaqodb.LoadAll<MyEntity>();
                Console.WriteLine("UpdateSiaqodb...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                foreach(MyEntity en in all)
                {
                    en.IntValue++;
                    siaqodb.StoreObject(en);
                }

                stopwatch.Stop();
                Console.WriteLine("UpdateSiaqodb took:" + stopwatch.Elapsed);

            }

            using (var dbsql = new SQLite.SQLiteConnection(sqLitePath))
            {
                var all = dbsql.Query<MyEntity>("select * from MyEntity");

                Console.WriteLine("UpdateSQLite...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach (MyEntity en in all)
                {
                    en.IntValue++;

                    dbsql.Update(en);
                }

                stopwatch.Stop();
                Console.WriteLine("UpdateSQLite took:" + stopwatch.Elapsed);
            }
        }
        public static void Read()
        {
            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 100 * OneMB, 20);
                Console.WriteLine("ReadAllSiaqodb...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                var all = siaqodb.LoadAll<MyEntity>();

                stopwatch.Stop();
                Console.WriteLine("ReadAllSiaqodb took:" + stopwatch.Elapsed);

            }

            using (var dbsql = new SQLite.SQLiteConnection(sqLitePath))
            {
                Console.WriteLine("ReadAllSQLite...");
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                var all = dbsql.Query<MyEntity>("select * from MyEntity");

                stopwatch.Stop();
                Console.WriteLine("ReadAllSQLite took:" + stopwatch.Elapsed);
            }
        }
        
       
        public static void Delete()
        {
            using (Siaqodb siaqodb = new Siaqodb())
            {
                siaqodb.Open(siaqodbPath, 100 * OneMB, 20);
                Console.WriteLine("DeleteSiaqodb...");
                var all = siaqodb.LoadAll<MyEntity>();
                var stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach(MyEntity en in all)
                {
                    siaqodb.Delete(en);
                }

                stopwatch.Stop();
                Console.WriteLine("DeleteSiaqodb took:" + stopwatch.Elapsed);

            }

            using (var dbsql = new SQLite.SQLiteConnection(sqLitePath))
            {
                Console.WriteLine("DeleteSQLite...");
                var all = dbsql.Query<MyEntity>("select * from MyEntity");

                var stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach (MyEntity en in all)
                {
                    dbsql.Delete(en);
                }
                stopwatch.Stop();
                Console.WriteLine("DeleteSQLite took:" + stopwatch.Elapsed);
            }
        }

        public static IEnumerable<MyEntity> GetEntities()
        {
            var random = new Random(DateTime.Now.Millisecond);

            for (int i = 0; i < ENTITY_COUNT; i++)
            {

                yield return new MyEntity
                {
                    IntValue = random.Next(),
                    DoubleValue = random.NextDouble(),
                    StringValue = Guid.NewGuid().ToString(),
                    DateTimeValue = new DateTime(random.Next(1999, 2015), random.Next(1, 12), random.Next(1, 28)),
                    GuidValue = Guid.NewGuid()
                };
            }
        }
    }
    public class MyEntity
    {
        [SQLite.AutoIncrement, SQLite.PrimaryKey]
        public int OID { get; set; }
        public int IntValue { get; set; }
        public string StringValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public Guid GuidValue { get; set; }
        public double DoubleValue { get; set; }
    }
}
