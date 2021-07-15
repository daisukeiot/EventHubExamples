using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.Functions.Worker;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace EventHubs.Sample.Legacy.Net
{
    public static class ConsumerLegacyNet
    {
        [Function("ConsumerLegacyNet")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub", Connection = "EventHubConnectionString")] string[] messages,
            DateTime[] enqueuedTimeUtcArray,
            long[] sequenceNumberArray,
            string[] offsetArray,
            Dictionary<string, JsonElement>[] propertiesArray,
            Dictionary<string, JsonElement>[] systemPropertiesArray,
            FunctionContext context)
        {
            // Unlike .Net Core, .Net does not bind to EventData
            // Message is passed as an array of strings.
            // Metadata can be obtained by adding more parameters, or through FunctionContext.
            // FunctionContext context)
            //
            // var systemPropertiesArray = context.BindingContext.BindingData["systemPropertiesArray"];
            //
            var exceptions = new List<Exception>();
            var log = context.GetLogger("ConsumerLegacyNet");

            for (int i = 0; i < messages.Length; i++)
            {
                string message = messages[i];
                DateTime enqueuedTimeUtc = enqueuedTimeUtcArray[i];
                long sequenceNumber = sequenceNumberArray[i];
                string offset = offsetArray[i];

                // Note: The values in these dictionaries are sent to the worker as JSON. By default, System.Text.Json will not automatically infer primitive values
                //       if you attempt to deserialize to 'object'. See https://docs.microsoft.com/en-us/dotnet/standard/serialization/system-text-json-migrate-from-newtonsoft-how-to?pivots=dotnet-5-0#deserialization-of-object-properties       
                //       
                //       If you want to use Dictionary<string, object> and have the values automatically inferred, you can use the sample JsonConverter specified
                //       here: https://docs.microsoft.com/en-us/dotnet/standard/serialization/system-text-json-converters-how-to?pivots=dotnet-5-0#deserialize-inferred-types-to-object-properties
                //
                //       See the Configuration sample in this repo for details on how to add this custom converter to the JsonSerializerOptions, or for 
                //       details on how to use Newtonsoft.Json, which does automatically infer primitive values.

                Dictionary<string, JsonElement> properties = propertiesArray[i];
                Dictionary<string, JsonElement> systemProperties = systemPropertiesArray[i];

                try
                {
                    string messageBody = messages[i];

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"Event From {systemProperties["iothub-connection-device-id"]} : {messageBody}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }
        }
    }
}
