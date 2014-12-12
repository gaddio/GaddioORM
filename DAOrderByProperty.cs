using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gaddio.ORM
{
    public class DAOrderByProperty
    {
        public string PropertyName { get; private set; }
        public DAOrderByTypes OrderByType { get; private set; }

        public DAOrderByProperty(string propertyName, DAOrderByTypes orderBy)
        {
            PropertyName = propertyName;
            OrderByType = orderBy;
        }
    }
}
