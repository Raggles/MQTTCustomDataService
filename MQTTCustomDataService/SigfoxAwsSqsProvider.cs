using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MQTTCustomDataService
{
    public class SigfoxAwsSqsProvider : DataProvider
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private Timer _t;
        private readonly AmazonSQSConfig _amazonSQSConfig;
        private AmazonSQSClient _sqsClient;
        private string _queueUrl = Properties.Settings.Default.SigfoxQueueURL;

        public SigfoxAwsSqsProvider(Action<string, string> publishData) : base(publishData)
        {
            try
            {
                _log.Info("Creating AwsSqsProvider...");
                _amazonSQSConfig = new AmazonSQSConfig
                {
                    ServiceURL = Properties.Settings.Default.SigfoxAwsSqsLocation
                };
                _sqsClient = new AmazonSQSClient(_amazonSQSConfig);
                _publishData = publishData;
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }

        public override void Start()
        {
            //TODO: validate the update time
            _t = new Timer(DoData, null, 0, Properties.Settings.Default.SigfoxUpdatePeriod);
        }

        /// <summary>
        /// Read all the messages from the queue and send them to the MQTT broker
        /// </summary>
        /// <param name="state"></param>
        private void DoData(object state)
        {
            try
            {
                var receiveMessageRequest = new ReceiveMessageRequest
                {
                    QueueUrl = _queueUrl,
                    MaxNumberOfMessages = 10
                };

                List<SigfoxMessage> messages = new List<SigfoxMessage>();
                int finishedCounter = 0;

                //Keep reading messages from the queue, until we get not results twice in a row.
                //https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
                while (finishedCounter < 2)
                {
                    var receiveMessageResponse = _sqsClient.ReceiveMessage(receiveMessageRequest);
                    if (receiveMessageResponse.Messages.Count == 0)
                        finishedCounter++;

                    foreach (var message in receiveMessageResponse.Messages)
                    {
                        try
                        {
                            _log.Debug(message.Body);
                            JObject dmsg = JObject.Parse(message.Body);
                            var smsg = new SigfoxMessage(dmsg)
                            {
                                RecieptHandle = message.ReceiptHandle
                            };
                            messages.Add(smsg);
                        }
                        catch (Exception ex)
                        {
                            _log.Warn(ex);
                        }
                    }
                }

                //order the messages by time so we send the data to the broker in the right order
                var orderedmessages = messages.OrderBy(x => x.TimeStamp).ToList();

                foreach (var message in orderedmessages)
                {
                    ProcessMessage(message);
                    DeleteMessage(message);
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }

        /// <summary>
        /// Delete a message from the message queue once we have processed it
        /// </summary>
        /// <param name="message"></param>
        private void DeleteMessage(SigfoxMessage message)
        {
            var deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = _queueUrl,
                ReceiptHandle = message.RecieptHandle
            };
            var response = _sqsClient.DeleteMessage(deleteMessageRequest);
        }

        /// <summary>
        /// Publish the message variables to MQTT broker
        /// </summary>
        /// <param name="message"></param>
        private void ProcessMessage(SigfoxMessage message)
        {
            try
            {
                _log.Debug(message);
                foreach (var kvp in message.Variables)
                {
                    object o = new { kvp.Value, Time = message.TimeStamp.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") };
                    _publishData($"sigfox/{message.Site}/{kvp.Key}", JsonConvert.SerializeObject(o));
                }
            }
            catch (Exception ex)
            {
                _log.Warn(ex);
            }
        }

        public override void Stop()
        {
            _t.Change(Timeout.Infinite, Timeout.Infinite);
        }
    }
}
