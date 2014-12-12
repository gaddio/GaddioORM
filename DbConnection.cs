using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;

namespace Gaddio.ORM
{
    public class DbConnection
    {
        private static string globalDatabaseName = string.Empty;
        private static string globalDatabaseHost = string.Empty;
        private static bool isProduction = false;

        public static string DatabaseName
        {
            get { return DbConnection.globalDatabaseName; }
            set { DbConnection.globalDatabaseName = value; }
        }

        public static string DatabaseHost
        {
            get
            {
                return DbConnection.globalDatabaseHost;
            }
            set
            {
                DbConnection.globalDatabaseHost = value;
            }
        }

        public static bool IsProduction
        {
            get { return DbConnection.isProduction; }
            set { DbConnection.isProduction = value; }
        }

        private static string GetConnectionString(string host, string name)
        {
            return "Server=" + host + "; Database=" + name + "; Integrated Security=True";
        }

        public static SqlConnection Get()
        {
            return new SqlConnection(DbConnection.GetConnectionString(DbConnection.globalDatabaseHost, DbConnection.globalDatabaseName));
        }

        public static SqlConnection GetSpecificConnection(string dbHost, string dbName)
        {
            return new SqlConnection(DbConnection.GetConnectionString(dbHost, dbName));
        }
    }
}
