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
            FlushAllDatabases();
        }

        [TestMethod]
        public void TestGetSetString()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();
            Assert.IsNull((string)db.StringGet(key));
            db.StringSet(key, "hello from redis");
            Assert.AreEqual((string)db.StringGet(key), "hello from redis");            
        }

        [TestMethod]
        public void TestGetSetByteKey()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToByteArray();
            Assert.IsNull((string)db.StringGet(key));
            db.StringSet(key, "byte key");
            Assert.AreEqual((string)db.StringGet(key), "byte key");
        }

        [TestMethod]
        public void TestGetSetInt()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var db = redis.GetDatabase();

            var key = Guid.NewGuid().ToString();
            Assert.IsNull((int?)db.StringGet(key));
            db.StringSet(key, 2703);
            Assert.AreEqual((int?)db.StringGet(key), 2703);
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

        [TestCleanup]
        public void TestCleanup()
        {
            FlushAllDatabases();
        }

        private void FlushAllDatabases()
        {
            var redis = ConnectionMultiplexer.Connect("localhost,allowAdmin=true");
            var endpoints = redis.GetEndPoints();
            var server = redis.GetServer(endpoints.First());
            server.FlushAllDatabases();
        }
    }
}
