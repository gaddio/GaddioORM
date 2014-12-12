using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gaddio.ORM
{
    public class DAUtil
    {
        public static Type ConvertTypeNameToType(string typeName)
        {
            switch (typeName.ToLower())
            {
                case "text":
                    return typeof(string);
                case "tinyint":
                    return typeof(byte);
                case "smallint":
                    return typeof(short);
                case "int":
                    return typeof(int);
                case "smalldatetime":
                    return typeof(DateTime);
                case "money":
                    return typeof(decimal);
                case "datetime":
                    return typeof(DateTime);
                case "date":
                    return typeof(DateTime);
                case "float":
                    return typeof(double);
                case "ntext":
                    return typeof(string);
                case "bit":
                    return typeof(bool);
                case "decimal":
                    return typeof(decimal);
                case "numeric":
                    return typeof(decimal);
                case "smallmoney":
                    return typeof(decimal);
                case "bigint":
                    return typeof(long);
                case "varchar":
                    return typeof(string);
                case "char":
                    return typeof(string);
                case "nvarchar":
                    return typeof(string);
                case "nchar":
                    return typeof(string);
                case "timestamp":
                    return typeof(double);
                default:
                    return null;
            }
        }
    }
}
