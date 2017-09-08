using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Runtime.Remoting.Contexts;
using System.Text;
using System.Threading.Tasks;
using Akka.Configuration;
using Akka.Pattern;
using Akka.Persistence.Sql.Common.Journal;
using MySql.Data.MySqlClient;

namespace Akka.Persistence.MySql.Journal
{
    public sealed class BatchingMySqlJournalSetup : BatchingSqlJournalSetup
    {
        
        /// <summary>
        /// Initializes a new instance of the <see cref="T:Akka.Persistence.MySql.Journal.BatchingMySqlJournalSetup" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string used to connect to the database.</param>
        /// <param name="maxConcurrentOperations">The maximum number of batch operations allowed to be executed at the same time.</param>
        /// <param name="maxBatchSize">The maximum size of single batch of operations to be executed over a single <see cref="T:System.Data.Common.DbConnection" />.</param>
        /// <param name="maxBufferSize">The maximum size of requests stored in journal buffer.</param>
        /// <param name="autoInitialize">
        /// If set to <c>true</c>, the journal executes all SQL scripts stored under the
        /// <see cref="P:Akka.Persistence.Sql.Common.Journal.BatchingSqlJournal`2.Initializers" /> collection prior
        /// to starting executing any requests.
        /// </param>
        /// <param name="connectionTimeout">The maximum time given for executed <see cref="T:System.Data.Common.DbCommand" /> to complete.</param>
        /// <param name="isolationLevel">The isolation level of transactions used during query execution.</param>
        /// <param name="circuitBreakerSettings">
        /// The settings used by the <see cref="T:Akka.Pattern.CircuitBreaker" /> when for executing request batches.
        /// </param>
        /// <param name="replayFilterSettings">The settings used when replaying events from database back to the persistent actors.</param>
        /// <param name="namingConventions">The naming conventions used by the database to construct valid SQL statements.</param>
        /// <param name="defaultSerializer">The serializer used when no specific type matching can be found.</param>
        public BatchingMySqlJournalSetup(string connectionString, int maxConcurrentOperations, int maxBatchSize,
            int maxBufferSize, bool autoInitialize, TimeSpan connectionTimeout, IsolationLevel isolationLevel,
            CircuitBreakerSettings circuitBreakerSettings, ReplayFilterSettings replayFilterSettings,
            QueryConfiguration namingConventions, string defaultSerializer) : base(connectionString,
            maxConcurrentOperations, maxBatchSize, maxBufferSize, autoInitialize, connectionTimeout, isolationLevel,
            circuitBreakerSettings, replayFilterSettings, namingConventions, defaultSerializer)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BatchingMySqlJournalSetup" /> class.
        /// </summary>
        /// <param name="config">The configuration used to configure the journal.</param>
        /// <exception cref="Akka.Configuration.ConfigurationException">
        /// This exception is thrown for a couple of reasons.
        /// <ul>
        /// <li>A connection string for the SQL event journal was not specified.</li>
        /// <li>
        /// An unknown <c>isolation-level</c> value was specified. Acceptable <c>isolation-level</c> values include:
        /// chaos | read-committed | read-uncommitted | repeatable-read | serializable | snapshot | unspecified
        /// </li>
        /// </ul>
        /// </exception>
        /// <exception cref="ArgumentNullException">
        /// This exception is thrown when the specified <paramref name="config"/> is undefined.
        /// </exception>
        public BatchingMySqlJournalSetup(Config config) : base(config, new QueryConfiguration(
            schemaName: config.GetString("schema-name"),
            journalEventsTableName: config.GetString("table-name"),
            metaTableName: config.GetString("metadata-table-name"),
            persistenceIdColumnName: "persistence_id",
            sequenceNrColumnName: "sequence_nr",
            payloadColumnName: "payload",
            manifestColumnName: "manifest",
            timestampColumnName: "created_at",
            isDeletedColumnName: "is_deleted",
            tagsColumnName: "tags",
            orderingColumnName: "ordering",
            serializerIdColumnName: "serializer_id",
            timeout: config.GetTimeSpan("connection-timeout"),
            defaultSerializer: config.GetString("default-serializer")))
        {
        }
    }

    public class BatchingMySqlJournal : BatchingSqlJournal<MySqlConnection, MySqlCommand>
    {
        
        public BatchingMySqlJournal(Config config) : base(new BatchingMySqlJournalSetup(config))
        {

        }

        public BatchingMySqlJournal(BatchingSqlJournalSetup setup) : base(setup)
        {
            var conventions = Setup.NamingConventions;
            Initializers = ImmutableDictionary.CreateRange(new[]
            {
                new KeyValuePair<string, string>("CreateJournalSql", $@"
                CREATE TABLE IF NOT EXISTS {conventions.FullJournalTableName} (
                    {conventions.OrderingColumnName} BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    {conventions.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {conventions.SequenceNrColumnName} BIGINT NOT NULL,
                    {conventions.IsDeletedColumnName} BIT NOT NULL,                    
                    {conventions.ManifestColumnName} VARCHAR(500) NOT NULL,
                    {conventions.TimestampColumnName} BIGINT NOT NULL,
                    {conventions.PayloadColumnName} LONGBLOB NOT NULL,
                    {conventions.TagsColumnName} VARCHAR(2000) NULL,
                    {conventions.SerializerIdColumnName} INT,
                    UNIQUE ({conventions.PersistenceIdColumnName}, {conventions.SequenceNrColumnName}),
                    INDEX journal_{conventions.SequenceNrColumnName}_idx ({conventions.SequenceNrColumnName}),
                    INDEX journal_{conventions.TimestampColumnName}_idx ({conventions.TimestampColumnName})
                );"),
                new KeyValuePair<string, string>("CreateMetadataSql", $@"
                CREATE TABLE IF NOT EXISTS {conventions.FullMetaTableName} (
                    {conventions.PersistenceIdColumnName} VARCHAR(255) NOT NULL,
                    {conventions.SequenceNrColumnName} BIGINT NOT NULL,
                    PRIMARY KEY ({conventions.PersistenceIdColumnName}, {conventions.SequenceNrColumnName})
                );"),
            });
        }

        protected override MySqlConnection CreateConnection(string connectionString) => new MySqlConnection(connectionString);

        protected override ImmutableDictionary<string, string> Initializers { get; }
    }
}
