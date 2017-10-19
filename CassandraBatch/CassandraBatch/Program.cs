using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CassandraBatch
{
    class Program
    {
        static bool Debug = false;
        static ISession Session { get; set; }
        static PreparedStatement _ps = null;
        static PreparedStatement Ps
        {
            get
            {
                if (_ps == null)
                {
                    _ps = Session.Prepare("INSERT INTO test (id, name, csharpDate, cqlDate, tuuid) VALUES (?, ?, ?, toUnixTimestamp(now()), now())");
                }

                return _ps;
            }
        }

        static void Main(string[] args)
        {
            /*
            var xxx1 = new TestModel()
            {
                writeMSecs = 1507642460269000,
                csharpDate = new DateTime(636432392600719326)
            };

            Console.WriteLine(xxx1);

            var xxx2 = new TestModel()
            {
                writeMSecs = 1507642186918803,
                csharpDate = new DateTime(636432389869023080)
            };

            Console.WriteLine(xxx2);

            //var dt1 = xxx.writeDateTime; // Write Time
            //var dt2 = new DateTime(636432373329021785); // UpdatesTsTicks
            //var diff = dt1 - dt2;
            */

            Console.WriteLine("Connecting to Cassandra...");

            Builder builder = Cluster.Builder()
                    .AddContactPoints("10.0.0.68")
                    .WithCompression(CompressionType.Snappy)
                    .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                    .WithCredentials("sys", "cassandra"); ;

            var cluster = builder.Build();
            Session = cluster.Connect("ospreypro_v1");

            Console.WriteLine("Connected");

            var tableTruncateCql = "TRUNCATE test";
            var tableDropCql = "DROP TABLE IF EXISTS test";
            var tableCreateCql = @"
CREATE TABLE test (
    id           int,
    name         text,
    csharpDate   timestamp,
    cqlDate      timestamp,
    tuuid        timeuuid,

    primary key (id)
);";

            Console.WriteLine("Dropping table...");

            Session.Execute(tableDropCql);

            Console.WriteLine("Dropped");

            Console.WriteLine("Creating table...");

            Session.Execute(tableCreateCql);

            Console.WriteLine("Created");

            //for (int i = 0; i <= 1; i += 4)
            //{
            //    Test(session, false, false, i + 1);
            //    Test(session, true, false, i + 2);
            //    Test(session, false, true, i + 3);
            //    Test(session, true, true, i + 4);
            //}

            var rec1 = ShowWriteTimes(true, false, 1);
            var rec2 = ShowWriteTimes(false, false, 2);

            var ts = rec2.writeDateTime - rec1.writeDateTime;

            Console.WriteLine("Write time diff: " + ts);
            Console.WriteLine("");

            Session.Execute(tableTruncateCql);

            ShowDoubleInserts(10, true, true, false);
            Console.WriteLine("");

            ShowDoubleInserts(20, false, false, false);
            Console.WriteLine("");

            ShowDoubleInserts(30, false, true, false);
            Console.WriteLine("");

            ShowDoubleInserts(40, true, false, false);
            Console.WriteLine("");

            Session.Execute(tableDropCql);

            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static void Insert(bool useBatch, bool useAsync, int id, string name = "")
        {
            if (Debug)
                Console.WriteLine("ENTER: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));

            if (Debug)
                Console.WriteLine(string.Format("Inserting: useBatch={0}, useAsync={1}, id={2}, name={3}", useBatch, useAsync, id, name));

            var bs = Ps.Bind(id, name, DateTime.UtcNow);
            IStatement statement;

            if (useBatch)
            {
                BatchStatement batch = new BatchStatement();

                batch.SetBatchType(BatchType.Logged);
                //batch.SetTimestamp(DateTimeOffset.UtcNow.AddSeconds(-1));

                batch.Add(bs);

                statement = batch;
            }
            else
            {
                statement = bs;
            }

            if (useAsync)
            {
                var task = Session.ExecuteAsync(statement);
                task.Wait();
            }
            else
            {
                Session.Execute(statement);
            }

            if (Debug)
                Console.WriteLine("EXIT: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));
        }

        private static TestModel Select(int id)
        {
            if (Debug)
                Console.WriteLine(string.Format("Selecting: id={0}", id));

            var rowset = Session.Execute("SELECT id, name, csharpDate, cqlDate, tuuid, writetime(tuuid) AS wt FROM test WHERE id = " + id);

            foreach (var row in rowset)
            {
                var test = new TestModel();

                test.id = row.GetValue<int>("id");
                test.name = row.GetValue<string>("name");
                test.csharpDate = row.GetValue<DateTime>("csharpdate");
                test.cqlDate = row.GetValue<DateTime>("cqldate");
                test.tuuid = row.GetValue<Guid>("tuuid");
                test.writeMSecs = row.GetValue<long>("wt");

                return test;
            }

            return null;
        }

        private static TestModel ShowWriteTimes(bool useBatch, bool useAsync, int id)
        {
            Console.WriteLine($"Running {"nameof(ShowWriteTimes)"} with useBatch={useBatch}");

            Insert(useBatch, useAsync, id);

            var rec = Select(id);

            if (rec == null)
            {
                Console.WriteLine("Record not found. Sleeping for 2 seconds...");

                Thread.Sleep(2000);

                rec = Select(id);

                if (rec == null)
                {
                    Console.WriteLine("Record still not found.");
                }
            }

            if (rec != null)
            {
                Console.WriteLine(rec);
            }

            return rec;
        }

        private static void ShowDoubleInserts(int id, bool useBatch1, bool useBatch2, bool useAsync)
        {
            Console.WriteLine($"Running {nameof(ShowDoubleInserts)} with useBatch1={useBatch1} and useBatch2={useBatch2} and useAsync={useAsync}");

            string name1 = "Name1";
            string name2 = "Name2";

            Insert(useBatch1, useAsync, id, name1);
            Insert(useBatch2, useAsync, id, name2);
            var rec = Select(id);

            Console.WriteLine($"> Name = {rec.name} (should be {name2})...{(rec.name == name2 ? "PASS" : "FAIL")}");
        }
    }
}
