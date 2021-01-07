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
        static string topicName = "test";
        static string groupName = "dotnetconsumer";
        static string connectionString = "Server=172.16.6.68;Database=CIMS;User Id=debezium;Password=Debetest01;";
        static async Task Main(string[] args)
        {
            //CreateCommand("USE CIMS CREATE TABLE Test (ClaimID int, Description varchar(255), FeeSubmitted money, TotalOwed money, State varchar(255), Paid bit);");
            await Task.Run(() => Consumer());
            /* Dictionary<string, object> groupfields = new Dictionary<string, object>();
            groupfields.Add("@ContractID", -1); // Set to -1 when creating new group sproc creates the ContractID
            groupfields.Add("@ContractType", 0); // 0-4, comes from Reference.ContractType (0=group) or DBNull.Value
            groupfields.Add("@CompanyName", "asdf"); // Required
            groupfields.Add("@ContactName1", ""); // not required, default is empty string
            groupfields.Add("@ContactName2", ""); // not required, default is empty string
            groupfields.Add("@AssociationID", DBNull.Value); // from Reference.Association or DBNull.Value
            groupfields.Add("@UserID", "jwiebe"); // Required, User that submitted the group creation
            groupfields.Add("@TimeStamp", DateTime.Now); // Current time
            groupfields.Add("@MaxDependentAge", null); // Defaults to null when not specified not required in message
            groupfields.Add("@MaxStudentAge", null); // Defaults to null when not specified not required in message
            groupfields.Add("@ExternalDesc", null); // Defaults to null when not specified not required in message
            groupfields.Add("@GroupClassificationID", null); // Defaults to null, Set by querying table Reference.GroupClassification (This happends outside of the SaveGroup() method)
            groupfields.Add("@GroupFeed", false); // Defaults to false when not specified.
            groupfields.Add("@GroupSendDate", DBNull.Value); // MUST BE THIS TYPE OF NULL WHEN NOT SPECIFIED
            Object contractID = CallSPROC(groupfields, "[dbo].[Customer_SaveGroup]")[0];

            Dictionary<string, object> emailfields = new Dictionary<string, object>();
            emailfields.Add("@ContractID", contractID); // Comes from group creation
            emailfields.Add("@EmailID", -1); // auto generated, -1 means it is new to cims inputoutput
            emailfields.Add("@EmailAddress", "test@test1.ca"); // comes from primary or admin
            emailfields.Add("@EmailAddressType", 1); // 1 is primary, 2 is admin
            emailfields.Add("@UserID", "jwiebe"); // user that created the request
            emailfields.Add("@TimeStamp", DateTime.Now); // Current time
            Object emailID = CallSPROC(emailfields, "[dbo].[Customer_SaveContractEmail]")[0];

            Dictionary<string, object> phonefields = new Dictionary<string, object>();
            phonefields.Add("@ContractID", contractID); // Comes from group creation
            phonefields.Add("@PhoneTypeID", 0); // 0-4 comes from Reference.PhoneType
            phonefields.Add("@PhoneID", -1); // send -1 for new phoneID inputoutput
            phonefields.Add("@AreaCode", 306); // area code int
            phonefields.Add("@Number", "1234567"); // phone number varchar of 7 numbers
            phonefields.Add("@UserID", "jwiebe"); // user that created the request
            phonefields.Add("@TimeStamp", DateTime.Now); // Current time
            Object phoneID = CallSPROC(phonefields, "[dbo].[Customer_SaveContractPhone]")[0];

            Dictionary<string, object> addressfields = new Dictionary<string, object>();
            addressfields.Add("@ContractID", contractID); // Comes from group creation
            addressfields.Add("@AddressTypeID", 1); // 0-3 comes from Reference.AddressType
            addressfields.Add("@AddressID", -1); // send -1 for new AddressID inputoutput
            addressfields.Add("@AddressLine1", "123 Street"); // address line 1
            addressfields.Add("@AddressLine2", ""); // address line 2
            addressfields.Add("@CountryID", 1); // 1-405 number represents country (1=canada)
            addressfields.Add("@City", "Regina"); // City
            addressfields.Add("@ProvID", 1); // 1-120 Represents province (1=Sask)
            addressfields.Add("@PostalCode", "S4W0T6"); // Postal Code
            addressfields.Add("@UserID", "jwiebe"); // user that created the request
            addressfields.Add("@TimeStamp", DateTime.Now); // Current time
            Object addressID = CallSPROC(addressfields, "[dbo].[Customer_SaveContractAddress]")[0]; */
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
                            List<Dictionary<string, object>> fields = GetFields(consumeResult.Message.Value);
                            CallGroupSPROCS(fields);
                            Console.WriteLine("Test");
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

        static List<Dictionary<string, object>> GetFields(GenericRecord message)
        {
            List<Dictionary<string, object>> listOfFields = new List<Dictionary<string, object>>();
            Dictionary<string, object> fields = new Dictionary<string, object>();
            foreach (Avro.Field m in message.Schema)
            {
                if (message.GetValue(m.Pos) is Object[])
                {
                    Object[] array = (Object[])message.GetValue(m.Pos);
                    foreach (GenericRecord element in array)
                    {
                        foreach (Dictionary<string, object> field in GetFields(element))
                        {
                            listOfFields.Add(field);
                        }
                    }
                }
                else fields.Add((m.Name), message.GetValue(m.Pos));
            }
            listOfFields.Add(fields);
            return listOfFields;
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
            else if (typeString.Equals("smallint")) return SqlDbType.SmallInt;
            else if (typeString.Equals("char")) return SqlDbType.Char;
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
            if (type.Count == 0) return null;
            else return type[0][0].ToString();
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

        static void CallGroupSPROCS(List<Dictionary<string, object>> listOfFields)
        {
            // --- Prepare fields ---
            // put @ before each key
            // set contractID for the address fields
            // set userID for the address fields
            // set timestamp for all fields
            // call SPROCS
            // ???
            // profit
            string userID = "";
            int contractID = -1;

            for (int i = 0; i < listOfFields.Count; i++)
            {
                listOfFields[i] = listOfFields[i].ToDictionary(d => !d.Key.Contains("@") ? "@" + d.Key : d.Key,
                                    d => d.Key == "TimeStamp" ? DateTime.Now : d.Value);
                listOfFields[i] = listOfFields[i].ToDictionary(d => d.Key,
                                    d => (d.Key == "@AssociationID" || d.Key == "@GroupSendDate") && d.Value == null ? DBNull.Value : d.Value);
                if (listOfFields[i].ContainsKey("@EmailAddresses")) listOfFields[i].Remove("@EmailAddresses");
                if (listOfFields[i].ContainsKey("@PhoneNumbers")) listOfFields[i].Remove("@PhoneNumbers");
                if (listOfFields[i].ContainsKey("@Addresses")) listOfFields[i].Remove("@Addresses");
                if (listOfFields[i].ContainsKey("@UserID")) userID = (String)listOfFields[i]["@UserID"];
                if (!listOfFields[i].ContainsKey("@TimeStamp")) listOfFields[i]["@TimeStamp"] = DateTime.Now;
                if (!listOfFields[i].ContainsKey("@UserID") && userID != "") listOfFields[i]["@UserID"] = userID;
                if (listOfFields[i].ContainsKey("@UserID") && listOfFields[i].ContainsKey("@ContractType") && contractID == -1) { contractID = SaveGroup(listOfFields[i]); listOfFields.Remove(listOfFields[i]); i -= 1; }
                if (!listOfFields[i].ContainsKey("@ContractID") && contractID != -1) listOfFields[i]["@ContractID"] = contractID;
                if (listOfFields[i].ContainsKey("@PhoneNumber")) {
                    listOfFields[i]["@AreaCode"] = Int32.Parse(listOfFields[i]["@PhoneNumber"].ToString().Substring(0,3));
                    listOfFields[i]["@Number"] = listOfFields[i]["@PhoneNumber"].ToString().Replace("-", String.Empty).Substring(3, 7);
                    listOfFields[i].Remove("@PhoneNumber");
                }
                
                if (listOfFields[i].ContainsKey("@UserID") && listOfFields[i].ContainsKey("@AddressID") && (int) listOfFields[i]["@AddressID"] == -1) { SaveContractAddress(listOfFields[i]); listOfFields.Remove(listOfFields[i]); i -= 1; }
                else if (listOfFields[i].ContainsKey("@UserID") && listOfFields[i].ContainsKey("@EmailID") && (int) listOfFields[i]["@EmailID"] == -1) { SaveContractEmail(listOfFields[i]); listOfFields.Remove(listOfFields[i]); i -= 1; }
                else if (listOfFields[i].ContainsKey("@UserID") && listOfFields[i].ContainsKey("@PhoneID") && (int) listOfFields[i]["@PhoneID"] == -1) { SaveContractPhone(listOfFields[i]); listOfFields.Remove(listOfFields[i]); i -= 1; }
                
                if (listOfFields.Count > 0 && i >= listOfFields.Count - 1) i = -1;
            }
        }
        static int SaveGroup(Dictionary<string, object> fields)
        {
            return (int)CallSPROC(fields, "[dbo].[Customer_SaveGroup]")[0];
        }
        static void SaveContractEmail(Dictionary<string, object> fields)
        {
            CallSPROC(fields, "[dbo].[Customer_SaveContractEmail]");
        }
        static void SaveContractAddress(Dictionary<string, object> fields)
        {
            CallSPROC(fields, "[dbo].[Customer_SaveContractAddress]");
        }
        static void SaveContractPhone(Dictionary<string, object> fields)
        {
            CallSPROC(fields, "[dbo].[Customer_SaveContractPhone]");
        }
    }
}