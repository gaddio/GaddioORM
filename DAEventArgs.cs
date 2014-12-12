using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Gaddio.ORM;

namespace Gaddio.ORM
{
    public delegate void DAEventHandler(object sender, DAEventArgs e);

    public class DAEventArgs
    {
        public DAEventArgs(object item, DAEventTypes eventType, DATransaction tx)
        {
            this.item = item;
            this.eventType = eventType;
            this.tx = tx;
        }

        private DATransaction tx;

        public DATransaction Tx
        {
            get { return tx; }
            set { tx = value; }
        }

        private DAEventTypes eventType = DAEventTypes.Unknown;

        public DAEventTypes EventType
        {
            get { return eventType; }
            set { eventType = value; }
        }

        private object item;

        public object Item
        {
            get { return item; }
            set { item = value; }
        }
    }
}
