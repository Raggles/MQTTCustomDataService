using System;

namespace MQTTCustomDataService
{
    public abstract class DataProvider
    {
        protected Action<string, string> _publishData;

        public DataProvider(Action<string, string> publishData)
        {
            _publishData = publishData;
        }

        abstract public void Start();
        abstract public void Stop();
    }
}
