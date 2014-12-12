using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gaddio.ORM
{
    public class DAMatchProperty
    {
        public string PropertyName { get; private set; }
        public DAMatchTypes MatchType { get; private set; }

        public DAMatchProperty(string propertyName, DAMatchTypes matchType)
        {
            PropertyName = propertyName;
            MatchType = matchType;
        }

        internal string Comparator
        {
            get
            {
                switch (MatchType)
                {
                    case DAMatchTypes.Equals: return "=";
                    case DAMatchTypes.GreaterThan: return ">";
                    case DAMatchTypes.GreaterThanOrEqual: return ">=";
                    case DAMatchTypes.LessThan: return "<";
                    case DAMatchTypes.LessThanOrEqual: return "<=";
                    case DAMatchTypes.Contains: return "LIKE";
                    case DAMatchTypes.StartsWith: return "LIKE";
                    case DAMatchTypes.EndsWith: return "LIKE";
                    case DAMatchTypes.NotEqual: return "<>";
                    default: throw new ApplicationException("Unsupported MatchTypes value: " + MatchType.ToString());
                }
            }
        }
    }
}
