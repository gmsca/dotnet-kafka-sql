using System;
using System.Threading;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Avro.Generic;

namespace kafka_consumer_sql
{
    class Program
    {
        static string bootstrapServers = "localhost:9092";
        static string schemaRegistryUrl = "http://localhost:8081";
        static string topicName = "group-topic";
        static string groupName = "dotnetconsumer";
        static string connectionString = "Server=172.16.6.68;Database=CIMS;User Id=debezium;Password=Debetest01;";
        static async Task Main(string[] args)
        {
            //CreateCommand("USE CIMS CREATE TABLE Test (ClaimID int, Description varchar(255), FeeSubmitted money, TotalOwed money, State varchar(255), Paid bit);");
            //await Task.Run(() => Consumer());
            Dictionary<string, object> fields = new Dictionary<string, object>();
            fields.Add("@ContractID", -1); // Set to -1 when creating new group sproc creates the ContractID
            fields.Add("@ContractType", 0); // If not defined, default is 0
            fields.Add("@CompanyName", "asdf"); // Required
            fields.Add("@ContactName1", ""); // not required, default is empty string
            fields.Add("@ContactName2", ""); // not required, default is empty string
            fields.Add("@AssociationID", DBNull.Value); // MUST BE THIS TYPE OF NULL WHEN NOT SPECIFIED
            fields.Add("@UserID", "jwiebe"); // Required, User that submitted the group creation
            fields.Add("@TimeStamp", DateTime.Now); // Current time
            fields.Add("@MaxDependentAge", null); // Defaults to null when not specified not required in message
            fields.Add("@MaxStudentAge", null); // Defaults to null when not specified not required in message
            fields.Add("@ExternalDesc", null); // Defaults to null when not specified not required in message
            fields.Add("@GroupClassificationID", null); // Defaults to null, Set by querying table Reference.GroupClassification (This happends outside of the SaveGroup() method)
            fields.Add("@GroupFeed", false); // Defaults to false when not specified.
            fields.Add("@GroupSendDate", DBNull.Value); // MUST BE THIS TYPE OF NULL WHEN NOT SPECIFIED
            Object ContractID = CallSPROC(fields, "[dbo].[Customer_SaveGroup]")[0];
            
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
                            Dictionary<string, object> fields = GetFields(consumeResult.Message.Value);
                            CallSPROC(fields, "[dbo].[Customer_SaveGroup]");
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

        static Dictionary<string, object> GetFields(GenericRecord message)
        {
            Dictionary<string, object> fields = new Dictionary<string, object>();
            foreach (Avro.Field m in message.Schema)
            {
                fields.Add(("@" + m.Name), message.GetValue(m.Pos));
            }
            return fields;
        }

        static List<object> CallSPROC(Dictionary<string, object> fields, string sprocName)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(sprocName, connection))
                {
                    command.CommandType = CommandType.StoredProcedure;

                    List<SqlParameter> outputParameters = new List<SqlParameter>();
                    List<String> outputFields = GetSPROCOutputs(sprocName);
                    foreach (KeyValuePair<string, object> kvp in fields)
                    {
                        SqlDbType type = GetSqlDbType(sprocName, kvp.Key);
                        if (outputFields.Contains(kvp.Key))
                        {
                            outputParameters.Add(command.Parameters.Add(kvp.Key, type));
                            outputParameters[outputParameters.Count - 1].Value = kvp.Value;
                            outputParameters[outputParameters.Count - 1].Direction = ParameterDirection.InputOutput;
                        }
                        else
                        {
                            command.Parameters.Add(kvp.Key, type).Value = kvp.Value;
                        }

                    }

                    command.Connection.Open();
                    command.ExecuteNonQuery();
                    List<object> output = new List<object>();
                    foreach (SqlParameter parameter in outputParameters)
                    {
                        output.Add(parameter.Value);
                    }
                    return output;
                }
            }
        }

        static SqlDbType GetSqlDbType(string sprocName, string inputName)
        {
            string typeString = QuerySPROCInputType(sprocName, inputName);
            if (typeString.Equals("int")) return SqlDbType.Int;
            else if (typeString.Equals("varchar")) return SqlDbType.VarChar;
            else if (typeString.Equals("money")) return SqlDbType.Money;
            else if (typeString.Equals("bit")) return SqlDbType.Bit;
            else if (typeString.Equals("datetime")) return SqlDbType.DateTime;
            else
            {
                Console.WriteLine("Type not found!!!");
                Environment.Exit(1);
                return SqlDbType.VarChar;
            }
        }

        static string QuerySPROCInputType(string sprocName, string inputName)
        {
            List<List<Object>> type = Select($"select type_name(user_type_id) from sys.parameters where object_id = object_id(\'{sprocName}\') and name=\'{inputName}\'");
            return type[0][0].ToString();
        }
        static List<String> GetSPROCOutputs(string sprocName)
        {
            List<List<Object>> outputs = Select($"select name from sys.parameters where is_output=1 and object_id = object_id(\'{sprocName}\');");
            List<object> flattenedOutputs = outputs.SelectMany(x => x).ToList();
            return flattenedOutputs.Select(i => i.ToString()).ToList();
        }

        static List<List<Object>> Select(string commandString)
        {
            List<List<Object>> rows = new List<List<Object>>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(commandString, connection))
                {
                    connection.Open();
                    try
                    {
                        SqlDataReader dataReader = command.ExecuteReader();
                        if (dataReader.HasRows)
                        {
                            while (dataReader.Read())
                            {
                                List<Object> row = new List<Object>();
                                for (int i = 0; i < dataReader.FieldCount; i++) row.Add(dataReader.GetSqlValue(i));
                                rows.Add(row);
                            }
                        }
                        else Console.WriteLine("Query retrieved no rows");
                    }
                    catch (SqlException e)
                    {
                        Console.WriteLine(e);
                    }
                }
            }
            return rows;
        }
    }
}