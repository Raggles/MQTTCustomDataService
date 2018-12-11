using HtmlAgilityPack;
using Newtonsoft.Json;
using System;
using System.Globalization;
using System.Threading;

namespace MQTTCustomDataService
{
    internal class Em6DataProvider : DataProvider
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private Timer _t;

        public Em6DataProvider(Action<string, string> publishData) : base(publishData)
        {
            _log.Info("Creating Em6DataProvider...");
        }

        /// <summary>
        /// Scrape pricing data from the EM6 website and publish it to the MQTT broker
        /// </summary>
        /// <param name="state"></param>
        private void DoData(object state)
        {
            try
            {
                var url = "http://www.em6live.co.nz";
                var web = new HtmlWeb();
                var doc = web.Load(url);
                string time = doc.DocumentNode.SelectSingleNode("//*[@id='dateTime']").InnerText;
                DateTime dtime;
                try
                {
                    dtime = DateTime.ParseExact(time, "hh.mmtt ddd dd MMM yyyy", CultureInfo.InvariantCulture);
                }
                catch
                {
                    dtime = DateTime.ParseExact(time, "hh.mmtt ddd d MMM yyyy", CultureInfo.InvariantCulture);
                }

                for (int i = 1; i <= 13; i++)
                {
                    string price = doc.DocumentNode.SelectSingleNode($"//*[@id='region{i}']").InnerText;
                    price = price.Trim(new char[] { '$' });
                    var o1 = new { Price = price, Time = dtime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ") };
                    _publishData($"prices/region{i}", JsonConvert.SerializeObject(o1));

                }
            }
            catch (Exception ex)
            {
                _log.Error(ex);
            }
        }

        public override void Start()
        {
            _t = new Timer(DoData, null, 0, Properties.Settings.Default.Em6UpdatePeriod);
        }

        public override void Stop()
        {
            _t.Change(Timeout.Infinite, Timeout.Infinite);
        }

    }
}