using System;
using Confluent.Kafka;
using Avro.Generic;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace kafka_consumer_sql
{
    class Program
    {
        static string bootstrapServers = "localhost:9092";
        static string schemaRegistryUrl = "http://localhost:8081";
        static string topicName = "CLAIM_CLAIMSTATUS_CLAIMSTATE_JOINED";
        static string groupName = "dotnetconsumer";
        static string connectionString = "Server=172.16.6.68;Database=CIMS;User Id=debezium;Password=Debetest01;";
        static async Task Main(string[] args)
        {
            //CreateCommand("USE CIMS CREATE TABLE Test (ClaimID int, Description varchar(255), FeeSubmitted money, TotalOwed money, State varchar(255), Paid bit);");
            await Task.Run(() => Consumer());
        }

        static void Consumer()
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            using (var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = schemaRegistryUrl }))
            using (var consumer =
                new ConsumerBuilder<int, GenericRecord>(new ConsumerConfig
                {
                    BootstrapServers = bootstrapServers,
                    GroupId = groupName,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                })
                    .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Subscribe(topicName);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);
                            SendDataToSQL(consumeResult.Message.Value);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                        Thread.Sleep(1000);
                    }
                }
                catch (OperationCanceledException)
                {
                    // commit final offsets and leave the group.
                    consumer.Close();
                }
            }
            cts.Cancel();
        }

        static void SendDataToSQL(GenericRecord message)
        {
            Object ClaimID = message.GetValue(0);
            Object Description = message.GetValue(1);
            Object FeeSubmitted = message.GetValue(2);
            Object TotalOwed = message.GetValue(3);
            Object State = message.GetValue(4);
            Object Paid = message.GetValue(5);
            UpdateTestTable(ClaimID, Description, FeeSubmitted, TotalOwed, State, Paid);
        }

        static void UpdateTestTable(Object ClaimID, Object Description, Object FeeSubmitted, Object TotalOwed, Object State, Object Paid)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand("[dbo].[UpdateTest]", connection))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    command.Parameters.Add("@ClaimID", SqlDbType.Int).Value = ClaimID;
                    command.Parameters.Add("@Description", SqlDbType.VarChar).Value = Description;
                    command.Parameters.Add("@FeeSubmitted", SqlDbType.Money).Value = FeeSubmitted;
                    command.Parameters.Add("@TotalOwed", SqlDbType.Money).Value = TotalOwed;
                    command.Parameters.Add("@State", SqlDbType.VarChar).Value = State;
                    command.Parameters.Add("@Paid", SqlDbType.Bit).Value = Paid;
                    command.Parameters.Add("@StatementType", SqlDbType.VarChar).Value = "Insert";

                    command.Connection.Open();
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
