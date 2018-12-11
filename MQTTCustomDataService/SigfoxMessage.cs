using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MQTTCustomDataService
{

    public class SigfoxMessage
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public Dictionary<string, object> Variables = new Dictionary<string, object>();
        public int ID { get; set; }
        public MessageType Type { get; set; }
        public MessageSeverity Severity { get; set; }
        public string Device { get; set; }
        public DateTime TimeStamp { get; set; }
        public string Organisation { get; set; }
        public string Site { get; set; }
        public string Message { get; set; }
        public string RecieptHandle { get; set; }

        public override string ToString()
        {
            return $"DateTime: {TimeStamp}, ID: {ID}, Site: {Site}, Device: {Device}, Severity: {Severity}, Message: {Message}";
        }

        public SigfoxMessage() { }

        public SigfoxMessage(JObject dmsg)
        {
            ID = (int)dmsg["id"];
            Message = (string)dmsg["message"];
            Organisation = (string)dmsg["organisation"];
            Site = (string)dmsg["site"];
            TimeStamp = ParseDate((string)dmsg["timestamp"]);
            Enum.TryParse((string)dmsg["severity"], out MessageSeverity severity);
            Severity = severity;
            Enum.TryParse((string)dmsg["type"], out MessageType type);
            Type = type;
            Device = (string)dmsg["device"];
            Variables = (from d in ((JObject)dmsg["variables"]).Properties() select d).ToDictionary(i => i.Name, i => (object)i.Value);
        }

        private DateTime ParseDate(string date)
        {
            return DateTime.Parse(((string)date).Replace("-", "/")).ToLocalTime();
        }
    }

    public enum MessageType
    {
        Info,
        Alert
    }

    public enum MessageSeverity
    {
        Info,
        Warning,
        Critical
    }

}
