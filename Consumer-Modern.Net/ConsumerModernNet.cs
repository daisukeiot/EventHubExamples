using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace EventHubs.Sample.Modern.Net
{
    public static class ConsumerModernNet
    {
        [Function("ConsumerModernNet")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub", Connection = "EventHubConnectionString")] string[] events, FunctionContext context)
        {
            var exceptions = new List<Exception>();
            var log = context.GetLogger("ConsumerModernNet");

            log.LogInformation($"Received EventHubTrigger with {events.GetType()}");

            try
            {
                var bindingData = context.BindingContext.BindingData;
                var systemPropertiesArray = context.BindingContext.BindingData["systemPropertiesArray"];
                var jsonArray = JArray.Parse(systemPropertiesArray.ToString());

                for (int i = 0; i < events.Length; i++)
                {
                    var eventMessage = JsonDocument.Parse(events[i]);
                    dynamic jObject = JObject.Parse(jsonArray[i].ToString());
                    var deviceId = jObject["iothub-connection-device-id"];
                    log.LogInformation($"Event From {deviceId} : {eventMessage.RootElement.ToString()}");
                    await Task.Yield();
                }
            }
            catch (Exception e)
            {
                // We need to keep processing the rest of the batch - capture this exception and continue.
                // Also, consider capturing details of the message that failed processing so it can be processed again later.
                exceptions.Add(e);
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}