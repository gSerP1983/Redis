using System;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;

namespace Test.Redis
{
    [TestClass]
    public class DbTest
    {
        [TestInitialize]
        public void TestInitialize()
        {
        }

        [TestMethod]
        public void TestKeyExists()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();
            var key = Guid.NewGuid().ToString();

            Assert.IsNull((string)db.StringGet(key));
            Assert.IsFalse(db.KeyExists(key));

            db.StringSet(key, "val");
            Assert.IsTrue(db.KeyExists(key));

            db.KeyDelete(key);
            Assert.IsFalse(db.KeyExists(key));
        }

        [TestMethod]
        public void TestStringVal()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();
            Assert.IsNull((string)db.StringGet(key));
            db.StringSet(key, "hello from redis");
            Assert.AreEqual((string)db.StringGet(key), "hello from redis");            
        }

        [TestMethod]
        public void TestByteKey()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToByteArray();
            Assert.IsNull((string)db.StringGet(key));
            db.StringSet(key, "byte key");
            Assert.AreEqual((string)db.StringGet(key), "byte key");
        }

        [TestMethod]
        public void TestIntVal()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();
            Assert.IsNull((int?)db.StringGet(key));
            db.StringSet(key, 2703);
            Assert.AreEqual((int?)db.StringGet(key), 2703);
        }

        [TestMethod]
        public void TestGetSet()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();
            Assert.IsNull((string)db.StringGet(key));
            db.StringSet(key, "val1");
            Assert.AreEqual((string)db.StringGetSet(key, "val2"), "val1");
            Assert.AreEqual((string)db.StringGet(key), "val2");
        }

        [TestMethod]
        public void TestExpire1()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();

            db.StringSet(key, "Expire Test1");
            db.KeyExpire(key, TimeSpan.FromSeconds(0.1));
            Thread.Sleep(10);
            Assert.AreEqual((string)db.StringGet(key), "Expire Test1");
            Assert.IsNotNull(db.StringGetWithExpiry(key).Expiry);
            Thread.Sleep(100);
            Assert.IsNull((string)db.StringGet(key));
        }

        [TestMethod]
        public void TestExpire2()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();

            db.StringSet(key, "Expire Test2", TimeSpan.FromSeconds(0.1));
            Thread.Sleep(10);
            Assert.AreEqual((string)db.StringGet(key), "Expire Test2");
            Assert.IsNotNull(db.StringGetWithExpiry(key).Expiry);
            Thread.Sleep(100);
            Assert.IsNull((string)db.StringGet(key));
        }

        [TestMethod]
        public void TestFireAndForget()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();
            Assert.IsNull((string)db.StringGet(key));
            db.StringSet(key, "fire and forget", flags: CommandFlags.FireAndForget);
            Thread.Sleep(10);
            Assert.AreEqual((string)db.StringGet(key), "fire and forget");
        }

        [TestMethod]
        public void TestPipelining()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key1 = Guid.NewGuid().ToString();
            var key2 = Guid.NewGuid().ToString();
            var key3 = Guid.NewGuid().ToString();

            db.StringSet(key1, "val1");
            db.StringSet(key2, "val2");
            db.StringSet(key3, "val3");

            var task1 = db.StringGetAsync(key1);
            var task2 = db.StringGetAsync(key2);
            var task3 = db.StringGetAsync(key3);

            var res1 = (string)db.Wait(task1);
            var res2 = (string)db.Wait(task2);
            var res3 = (string)db.Wait(task3);

            Assert.AreEqual(res1, "val1");
            Assert.AreEqual(res2, "val2");
            Assert.AreEqual(res3, "val3");
        }

        [TestMethod]
        public void TestStringKeyType()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();
            db.StringSet(key, "string");
            Assert.AreEqual(db.KeyType(key), RedisType.String);

            db.StringSet(key, 12.56);
            Assert.AreEqual(db.KeyType(key), RedisType.String);
        }

        [TestMethod]
        public void TestRename()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();
            var key1 = Guid.NewGuid().ToString();
            var key2 = Guid.NewGuid().ToString();

            db.StringSet(key1, "val");
            Assert.AreEqual((string) db.StringGet(key1), "val");
            Assert.IsTrue(db.KeyExists(key1));

            db.KeyRename(key1, key2);
            Assert.AreEqual((string)db.StringGet(key2), "val");
            Assert.IsFalse(db.KeyExists(key1));
            Assert.IsTrue(db.KeyExists(key2));
        }

        [TestMethod]
        public void TestKeyPrefix()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var endpoints = redis.GetEndPoints();
            var server = redis.GetServer(endpoints.First());
            var db = redis.GetDatabase();

            var keys = new[] { "prefix-1-" + Guid.NewGuid(), "prefix-2-" + Guid.NewGuid() };
            var arr = new [] { "val-1", "val-2" };
            db.StringSet(keys[0], arr[0]);
            db.StringSet(keys[1], arr[1]);

            var keyScan = server.Keys(pattern: "prefix*").ToArray();
            Assert.AreEqual(2, keyScan.Length);
            Assert.IsTrue(keys.Contains(keyScan[0].ToString()));
            Assert.IsTrue(keys.Contains(keyScan[1].ToString()));
            Assert.IsTrue(arr.Contains((string)db.StringGet(keyScan[0])));
            Assert.IsTrue(arr.Contains((string)db.StringGet(keyScan[1])));

            db.KeyDelete(keyScan[0]);
            keyScan = server.Keys(pattern: "prefix*").ToArray();
            Assert.AreEqual(1, keyScan.Length);
        }


        [TestMethod]
        public void ZTestFlushAllDatabases()
        {
            var redis = ConnectionMultiplexer.Connect("localhost,allowAdmin=true");
            var endpoints = redis.GetEndPoints();
            var server = redis.GetServer(endpoints.First());

            var db = redis.GetDatabase();
            db.StringSet(Guid.NewGuid().ToString(), "val1");

            var len = server.Keys(pattern: "*").ToArray().Length;
            Assert.IsTrue(len > 0);
            server.FlushAllDatabases();
            Assert.AreEqual(0, server.Keys(pattern: "*").ToArray().Length);
        }


        [TestCleanup]
        public void TestCleanup()
        {
        }

    }
}
