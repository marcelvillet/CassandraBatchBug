using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;

namespace CassandraBatch
{
    class Program
    {
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
                    .WithDefaultKeyspace("ospreypro_mv")
                    .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                    .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                    .WithCredentials("sys", "cassandra"); ;

            var cluster = builder.Build();
            var session = cluster.Connect("ospreypro_mv");

            Console.WriteLine("Connected");

            var tableDropCql = "DROP TABLE IF EXISTS test";
            var tableCreateCql = @"
CREATE TABLE test (
    id           int,
    csharpDate   timestamp,
    cqlDate      timestamp,
    tuuid        timeuuid,

    primary key (id)
);";

            Console.WriteLine("Dropping table...");

            session.Execute(tableDropCql);

            Console.WriteLine("Dropped");

            Console.WriteLine("Creating table...");

            session.Execute(tableCreateCql);

            Console.WriteLine("Created");

            //for (int i = 0; i <= 1; i += 4)
            //{
            //    Test(session, false, false, i + 1);
            //    Test(session, true, false, i + 2);
            //    Test(session, false, true, i + 3);
            //    Test(session, true, true, i + 4);
            //}

            var rec1 = Test(session, true, false, 1);
            var rec2 = Test(session, false, false, 2);

            var ts = rec2.writeDateTime - rec1.writeDateTime;

            Console.WriteLine("Write time diff: " + ts);

            session.Execute(tableDropCql);

            Console.WriteLine("Press ENTER to exit");
            Console.ReadLine();
        }

        private static void Insert(ISession session, bool useBatch, bool useAsync, int id)
        {
            Console.WriteLine("ENTER: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));
            
            Console.WriteLine(string.Format("Inserting: useBatch={0}, useAsync={1}, id={2}", useBatch, useAsync, id));

            var ps = session.Prepare("INSERT INTO test (id, csharpDate, cqlDate, tuuid) VALUES (?, ?, toUnixTimestamp(now()), now())");
            var bs = ps.Bind(id, DateTime.UtcNow);
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
                var task = session.ExecuteAsync(statement);
                task.Wait();
            }
            else
            {
                session.Execute(statement);
            }

            Console.WriteLine("EXIT: Insert " + DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fffZ"));
        }

        private static async Task InsertAsync(ISession session, bool useBatch, int id)
        {
            Console.WriteLine(string.Format("Inserting: useBatch={0}, id={1}", useBatch, id));

            var ps = session.Prepare("INSERT INTO test (id, csharpDate, cqlDate, tuuid) VALUES (?, ?, toUnixTimestamp(now()), now())");
            var bs = ps.Bind(id, DateTime.UtcNow);

            if (useBatch)
            {
                BatchStatement batch = new BatchStatement();

                batch.SetBatchType(BatchType.Logged);

                batch.Add(bs);

                await session.ExecuteAsync(bs);
            }
            else
            {
                await session.ExecuteAsync(bs);
            }
        }

        private static TestModel Select(ISession session, int id)
        {
            Console.WriteLine(string.Format("Selecting: id={0}", id));

            var rowset = session.Execute("SELECT id, csharpDate, cqlDate, tuuid, writetime(tuuid) AS wt FROM test WHERE id = " + id);

            foreach (var row in rowset)
            {
                var test = new TestModel();

                test.id = row.GetValue<int>("id");
                test.csharpDate = row.GetValue<DateTime>("csharpdate");
                test.cqlDate = row.GetValue<DateTime>("cqldate");
                test.tuuid = row.GetValue<Guid>("tuuid");
                test.writeMSecs = row.GetValue<long>("wt");

                Console.WriteLine(test);

                return test;
            }

            return null;
        }

        private static TestModel Test(ISession session, bool useBatch, bool useAsync, int id)
        {
            Console.WriteLine("Running test with useBatch=" + useBatch);

            Insert(session, useBatch, useAsync, id);

            var rec = Select(session, id);

            if (rec == null)
            {
                Console.WriteLine("Record not found. Sleeping for 2 seconds...");

                Thread.Sleep(2000);

                rec = Select(session, id);

                if (rec == null)
                {
                    Console.WriteLine("Record still not found.");
                }
            }

            return rec;
        }
    }
}
