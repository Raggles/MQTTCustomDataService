using System;
using System.Collections.Generic;
using System.Net;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace MQTTCustomDataService
{
    public partial class MQTTCustomDataService : ServiceBase
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType); 

        private List<MqttClient> _clients = new List<MqttClient>();
        private List<DataProvider> _providers = new List<DataProvider>();
        private object _updateSemaphore = new object();
        private Timer _t;

        public MQTTCustomDataService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                _log.Info("Custom Data Service is starting...");
                CreateClients();
                _providers.Add(new Em6DataProvider(UpdateMqttClients));
                _providers.Add(new SigfoxAwsSqsProvider(UpdateMqttClients));
                _log.Info("Starting Data Providers...");
                foreach (var provider in _providers)
                {
                    provider.Start();
                }
                _t = new Timer(ClientConnectionCheck, null, Properties.Settings.Default.MQTTClientTimerCheck, Properties.Settings.Default.MQTTClientTimerCheck);
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }

        private void ClientConnectionCheck(object state)
        {
            _log.Info("Client connection check");
            foreach (var c in _clients)
            {
                try
                {
                    if (!c.IsConnected)
                    {
                        _log.Warn($"Client {c.ClientId} is disconnected, attempting reconnect...");
                        byte b = c.Connect(c.ClientId);
                    }
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                }
            }
        }

        private void CreateClients()
        {
            var clients = Properties.Settings.Default.MQTTClients.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var c in clients)
            {
                // create client instance 
                MqttClient client = new MqttClient(IPAddress.Parse(c));
                _clients.Add(client);
                string clientId = Guid.NewGuid().ToString();
                try
                {
                    _log.Info($"Connecting to client {c}...");
                    byte b = client.Connect(clientId);
                    //_log.Info($"Client connect returned {b}");
                }
                catch (Exception ex)
                {
                    _log.Error(ex);
                }
            }
        }

        protected override void OnStop()
        {
            try
            {
                _log.Info("Custom Data Service is stopping...");
                foreach (var provider in _providers)
                {
                    provider.Stop();
                }
                foreach (var client in _clients)
                {
                    if (client.IsConnected)
                        client.Disconnect();
                }
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }

        public void UpdateMqttClients(string topic, string value)
        {
            lock (_updateSemaphore) //TODO: not sure if this is required, check if the client is threadsafe
            {
                _log.Debug($"topic:{topic} value:{value}");
                foreach (var client in _clients)
                {
                    if (client.IsConnected)
                    {
                        client.Publish(topic, Encoding.UTF8.GetBytes(value), MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, true);
                    }
                }
            }
        }
    }
}
