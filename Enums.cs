using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gaddio.ORM
{
    public enum DAEventTypes { Unknown, Create, Update, Delete }
    public enum DAMatchTypes { Equals, NotEqual, GreaterThan, LessThan, GreaterThanOrEqual, LessThanOrEqual, StartsWith, EndsWith, Contains, Unknown }
    public enum DAOrderByTypes { ASC, DESC }
    public enum DataAccessUtilTestModes { Production, Testing }
}
