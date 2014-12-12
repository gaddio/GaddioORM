using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Security.Principal;
using System.Data;
using System.Text.RegularExpressions;
using System.Reflection;
using System.Data.SqlTypes;

namespace Gaddio.ORM
{
    public class DA : DataAccessUtil
    {
        private static string TABLE_NAME_REGEX = @"[a-zA-Z0-9_]{1,}$";

        #region Abstract CRUD

        #region Events

        public static event DAEventHandler DAEventOccurred;
        private static void RaiseDAEvent(object obj, DAEventTypes eventType, DATransaction tx)
        {
            if (DAEventOccurred != null)
            {
                DAEventOccurred(null, new DAEventArgs(obj, eventType, tx));
            }
        }

        #endregion

        #region Create

        public static SqlCommand GetCreateCmd<T>(T item)
        {
            if (!IsInsertable<T>())
            {
                throw new DBTableNotInserableException();
            }
            string tableName = GetTableName<T>();

            List<PropertyNameValue> creatableValues = DA.ExtractKeyValues<T>(item, DBField.DBFieldKeys.Insertable);

            SqlCommand cmd = new SqlCommand();
            string creatableFieldNames = string.Empty;
            string creatableFieldValues = string.Empty;
            foreach (PropertyNameValue tup in creatableValues)
            {
                creatableFieldNames += ("" + tup.ColumnName + ",");
                creatableFieldValues += ("@" + tup.PropertyName + ",");

                cmd.Parameters.AddWithValue(tup.PropertyName, tup.Obj);
            }
            creatableFieldNames = creatableFieldNames.Substring(0, creatableFieldNames.Length - 1);
            creatableFieldValues = creatableFieldValues.Substring(0, creatableFieldValues.Length - 1);

            cmd.CommandText = @"

INSERT INTO " + tableName + @" 
(" + creatableFieldNames + @")
VALUES
(" + creatableFieldValues + @")";

            if (HasAutoIncrement<T>(item))
            {
                cmd.CommandText += @"

SELECT CAST(SCOPE_IDENTITY() as int)
";
            }

            return cmd;
        }

        public static int Create<T>(T item)
        {
            return Create<T>(item, default(DATransaction));
        }

        public static int Create<T>(T item, DATransaction tx)
        {
            return Create<T>(item, tx, x => { return x; });
        }

        public delegate int CreateWithIdentityHandler(int identityId);

        public static int Create<T>(T item, CreateWithIdentityHandler handler)
        {
            return Create<T>(item, null, handler);
        }

        public static int Create<T>(T item, DATransaction tx, CreateWithIdentityHandler handler)
        {
            int id = 0;

            SqlCommand cmd = GetCreateCmd<T>(item);
            if (tx != null)
            {
                cmd.Transaction = tx.Tx;
            }

            if (HasAutoIncrement<T>(item))
            {
                if (cmd.Transaction != default(SqlTransaction))
                {
                    cmd.Connection = cmd.Transaction.Connection;
                    id = (int)cmd.ExecuteScalar();
                }
                else
                {
                    id = (int)DataAccessUtil.ExecScalar(cmd);
                }
            }
            else
            {
                if (cmd.Transaction != default(SqlTransaction))
                {
                    cmd.Connection = cmd.Transaction.Connection;
                    cmd.ExecuteNonQuery();
                }
                else
                {
                    DataAccessUtil.ExecNonQuery(cmd);
                }
            }

            RaiseDAEvent(item, DAEventTypes.Create, tx);

            return handler(id);
        }

        #endregion

        #region Read

        public static T Get<T>(T prototype, string[] propertyNames, DATransaction tx)
        {
            if (propertyNames == null || propertyNames.Length <= 0)
            {
                throw new PrimaryKeyPropertiesNotFoundException();
            }

            List<T> results = GetList<T>(prototype, 1, ToMatchProperties(propertyNames), null, tx);
            if (results.Count >= 1)
            {
                return results[0];
            }
            else
            {
                return default(T);
            }
        }

        public static T Get<T>(T prototype, params string[] propertyNames)
        {
            return Get<T>(prototype, propertyNames, null);
        }

        public static List<T> GetList<T>(T prototype, int? top, params string[] propertyNames)
        {
            return GetList<T>(prototype, top, ToMatchProperties(propertyNames), null);
        }

        public static List<T> GetList<T>(T prototype, int? top, string[] propertyNames, DATransaction tx)
        {
            return GetList(prototype, top, ToMatchProperties(propertyNames), null, tx);
        }

        public static List<T> GetList<T>(T prototype, int? top, DAMatchProperty[] properties, DAOrderByProperty[] orderByProperties)
        {
            return GetList<T>(prototype, top, properties, orderByProperties, null);
        }

        public static List<T> GetList<T>(T prototype, int? top, DAMatchProperty[] properties, DAOrderByProperty[] orderByProperties, DATransaction tx)
        {
            SqlCommand cmd = GetGetListCommand<T>(prototype, top, properties, orderByProperties);
            if (tx != null)
            {
                cmd.Transaction = tx.Tx;
            }

            if (cmd.Transaction != default(SqlTransaction))
            {
                List<T> results = new List<T>();

                cmd.Connection = cmd.Transaction.Connection;

                using (SqlDataReader dr = cmd.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        T item = Load<T>(dr, default(T));
                        results.Add(item);
                    }
                }

                return results;
            }
            else
            {
                return DataAccessUtil.ExecReaderLoadMultipleRecords<T>(cmd, Load<T>);
            }
        }

        public static SqlCommand GetGetListCommand<T>(T prototype, int? top, DAMatchProperty[] matchProperties, DAOrderByProperty[] orderByProperties)
        {
            string tableName = GetTableName<T>();

            SqlCommand cmd = new SqlCommand(@"

SELECT " + (!top.HasValue || top.Value <= 0 ? "" : ("TOP " + top.Value)) + @" * 
FROM " + tableName + " WHERE " + DA.GetWhereClause<T>(matchProperties) + @" " + DA.GetOrderByClause<T>(orderByProperties) + @"
");

            for (int i = 0; i < matchProperties.Length; ++i)
            {
                object val = GetPropertyValue<T>(prototype, matchProperties[i].PropertyName);

                if (val is string)
                {
                    if (matchProperties[i].MatchType == DAMatchTypes.StartsWith)
                    {
                        val = (string)val + "%";
                    }
                    else if (matchProperties[i].MatchType == DAMatchTypes.EndsWith)
                    {
                        val = "%" + (string)val;
                    }
                    else if (matchProperties[i].MatchType == DAMatchTypes.Contains)
                    {
                        val = "%" + (string)val + "%";
                    }
                }

                cmd.Parameters.AddWithValue("@" + matchProperties[i].PropertyName, val);
            }

            cmd.CommandType = System.Data.CommandType.Text;

            return cmd;
        }

        private static DAMatchProperty[] ToMatchProperties(string[] propertyNames)
        {
            List<DAMatchProperty> props = new List<DAMatchProperty>();
            if (propertyNames != null)
            {
                foreach (string prop in propertyNames)
                {
                    props.Add(new DAMatchProperty(prop, DAMatchTypes.Equals));
                }
            }

            return props.ToArray();
        }

        public static List<T> GetList<T>(T representative)
        {
            string tableName = GetTableName<T>();

            List<PropertyNameValue> keyValues = DA.ExtractKeyValues<T>(representative, DBField.DBFieldKeys.SelectableBy);

            SqlCommand cmd = new SqlCommand(@"

SELECT *
FROM " + tableName + " WHERE " + DA.GetWhereClause(keyValues) + @"
");

            for (int i = 0; i < keyValues.Count; ++i)
            {
                cmd.Parameters.AddWithValue("@" + keyValues[i].PropertyName, keyValues[i].Obj);
            }
            cmd.CommandType = System.Data.CommandType.Text;

            return DataAccessUtil.ExecReaderLoadMultipleRecords<T>(cmd, Load<T>);
        }

        public static List<T> GetList<T>()
        {
            string tableName = GetTableName<T>();

            SqlCommand cmd = new SqlCommand(@"
SELECT *
FROM " + tableName);

            cmd.CommandType = System.Data.CommandType.Text;
            return DataAccessUtil.ExecReaderLoadMultipleRecords<T>(cmd, Load<T>);
        }

        #endregion

        #region Update

        /// <summary>
        /// Generates UPDATE query for the item parameter using the properties in the WHERE clause
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item"></param>
        /// <param name="properties"></param>
        /// <returns></returns>
        public static SqlCommand GetUpdateCommand<T>(T item, params string[] properties)
        {
            if (!IsUpdateable<T>())
            {
                throw new DBTableNotUpdatableException();
            }
            if (properties == null ||
                properties.Length <= 0)
            {
                throw new ParameterListEmptyException();
            }

            string tableName = GetTableName<T>();

            List<PropertyNameValue> primaryKeyValues = DA.ExtractKeyValues<T>(item, DBField.DBFieldKeys.Primary);

            string setClause = "";

            foreach (string p in properties)
            {
                string safeColumnName = GetSafeColumnNameByProperty<T>(p);
                string safePropertyName = GetSafePropertyNameByProperty<T>(p);
                setClause += " " + safeColumnName + "=@" + safePropertyName + ",";
            }

            if (!string.IsNullOrWhiteSpace(setClause))
            {
                setClause = setClause.Substring(0, setClause.Length - 1);
            }
            else
            {
                throw new SetClauseEmptyException();
            }

            SqlCommand cmd = new SqlCommand(@"

UPDATE " + tableName + @" 

SET
" +

    setClause

  + @"

WHERE " + DA.GetWhereClause(primaryKeyValues) + @"
");

            foreach (string p in properties)
            {
                string safePropertyName = GetSafePropertyNameByProperty<T>(p);
                cmd.Parameters.AddWithValue("@" + safePropertyName, GetPropertyValue<T>(item, safePropertyName));
            }

            for (int i = 0; i < primaryKeyValues.Count; ++i)
            {
                cmd.Parameters.AddWithValue("@" + primaryKeyValues[i].PropertyName, primaryKeyValues[i].Obj);
            }

            cmd.CommandType = System.Data.CommandType.Text;

            return cmd;
        }

        public delegate T UpdateHandler<T>(T item);

        public static void Update<T>(T item, string[] properties, DATransaction tx, UpdateHandler<T> handler)
        {
            SqlCommand cmd = GetUpdateCommand<T>(item, properties);
            if (tx != null)
            {
                cmd.Transaction = tx.Tx;
            }

            if (cmd.Transaction != default(SqlTransaction))
            {
                cmd.Connection = cmd.Transaction.Connection;
                cmd.ExecuteNonQuery();
            }
            else
            {
                DataAccessUtil.ExecNonQuery(cmd);
            }

            if (handler != default(UpdateHandler<T>))
            {
                handler(item);
            }

            RaiseDAEvent(item, DAEventTypes.Update, tx);
        }

        public static void Update<T>(T item, string[] properties, UpdateHandler<T> handler)
        {
            Update<T>(item, properties, null, handler);
        }

        public static void Update<T>(T item, params string[] properties)
        {
            Update<T>(item, properties, default(DATransaction), default(UpdateHandler<T>));
        }


        #endregion
        
        #region Delete

        public delegate T DeleteHandler<T>(T item);

        public static void DeleteBy<T>(T item)
        {
            if (!IsDeletable<T>())
            {
                throw new DBTableNotDeletableException();
            }
            string tableName = GetTableName<T>();

            List<PropertyNameValue> keyValues = DA.ExtractKeyValues<T>(item, DBField.DBFieldKeys.DeletableBy);

            if (keyValues.Count <= 0)
            {
                throw new PrimaryKeyPropertiesNotFoundException();
            }

            SqlCommand cmd = new SqlCommand();

            string cmdStr = @"
DELETE FROM " + tableName + " WHERE ";

            foreach (PropertyNameValue t in keyValues)
            {
                cmdStr += " " + t.ColumnName + "=@" + t.PropertyName + " AND ";
                cmd.Parameters.AddWithValue("@" + t.PropertyName, t.Obj);
            }

            cmdStr = cmdStr.Substring(0, cmdStr.Length - 5);

            cmd.CommandText = cmdStr;
            cmd.CommandType = System.Data.CommandType.Text;
            DataAccessUtil.ExecNonQuery(cmd);
        }

        public static void Delete<T>(T item, DATransaction tx, DeleteHandler<T> handler)
        {
            if (!IsDeletable<T>())
            {
                throw new DBTableNotDeletableException();
            }
            string tableName = GetTableName<T>();

            List<PropertyNameValue> primaryKeyValues = DA.ExtractKeyValues<T>(item, DBField.DBFieldKeys.Primary);

            if (primaryKeyValues.Count <= 0)
            {
                throw new PrimaryKeyPropertiesNotFoundException();
            }

            SqlCommand cmd = new SqlCommand();

            string cmdStr = @"
DELETE FROM " + tableName + " WHERE ";

            foreach (PropertyNameValue t in primaryKeyValues)
            {
                cmdStr += " " + t.ColumnName + "=@" + t.PropertyName + " AND ";
                cmd.Parameters.AddWithValue("@" + t.PropertyName, t.Obj);
            }

            cmdStr = cmdStr.Substring(0, cmdStr.Length - 5);

            cmd.CommandText = cmdStr;
            cmd.CommandType = System.Data.CommandType.Text;

            if (tx != null)
            {
                cmd.Transaction = tx.Tx;
            }

            if (cmd.Transaction != default(SqlTransaction))
            {
                cmd.Connection = cmd.Transaction.Connection;
                cmd.ExecuteScalar();
            }
            else
            {
                DataAccessUtil.ExecNonQuery(cmd);
            }

            RaiseDAEvent(item, DAEventTypes.Delete, tx);
        }

        public static void Delete<T>(T item)
        {
            Delete<T>(item, null, x => { return x; });
        }

        #endregion

        #region Helper Functions

        public static bool EqualByPrimaryKey<T>(T a, T b)
        {
            if (a == null || b == null)
            {
                return false;
            }
            else
            {
                List<PropertyNameValue> aVals = ExtractKeyValues<T>(a, DBField.DBFieldKeys.Primary);
                List<PropertyNameValue> bVals = ExtractKeyValues<T>(b, DBField.DBFieldKeys.Primary);

                for (int i = 0; i < aVals.Count; ++i)
                {
                    if (!object.Equals(aVals[i].Obj, bVals[i].Obj))
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        private static bool HasAutoIncrement<T>(T item)
        {
            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (props != null)
            {
                foreach (PropertyInfo prop in props)
                {
                    object[] attrs = prop.GetCustomAttributes(typeof(DBField), false);
                    if (attrs != null && attrs.Length > 0)
                    {
                        if ((((DBField)attrs[0]).Key & DBField.DBFieldKeys.AutoIncrement) == DBField.DBFieldKeys.AutoIncrement)
                        {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        private static string GetSafePropertyNameByProperty<T>(string candidatePropertyName)
        {
            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (props != null)
            {
                foreach (PropertyInfo prop in props)
                {
                    object[] attrs = prop.GetCustomAttributes(typeof(DBField), false);
                    if (attrs != null && attrs.Length > 0)
                    {
                        if (string.Equals(prop.Name, candidatePropertyName, StringComparison.OrdinalIgnoreCase))
                        {
                            // found matching property on item, so use property name
                            return prop.Name;
                        }
                    }
                }
            }

            throw new DBFieldNotFoundException(candidatePropertyName);
        }

        private static string GetSafeColumnNameByProperty<T>(string candidatePropertyName)
        {
            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (props != null)
            {
                foreach (PropertyInfo prop in props)
                {
                    object[] attrs = prop.GetCustomAttributes(typeof(DBField), false);
                    if (attrs != null && attrs.Length > 0)
                    {
                        if (string.Equals(prop.Name, candidatePropertyName, StringComparison.OrdinalIgnoreCase))
                        {
                            // found matching property, so use fieldname if specified, or use property name
                            return string.IsNullOrWhiteSpace(((DBField)attrs[0]).FieldName) ? prop.Name : ((DBField)attrs[0]).FieldName;
                        }
                    }
                }
            }

            throw new DBFieldNotFoundException(candidatePropertyName);
        }

        public static string GetTableName<T>()
        {
            string tableName = Regex.Match(typeof(T).ToString(), TABLE_NAME_REGEX, RegexOptions.Singleline).Value;

            object[] attrs = typeof(T).GetCustomAttributes(typeof(DBTable), false);
            if (attrs != null && attrs.Length > 0 && !string.IsNullOrWhiteSpace(((DBTable)attrs[0]).TableName))
            {
                tableName = ((DBTable)attrs[0]).TableName;
            }
            else
            {
                // derive it from the name of the class
                if (tableName[tableName.Length - 1] == 's')
                {
                    tableName = tableName + "es";
                }
                else if (tableName[tableName.Length - 1] == 'y'
                            && tableName[tableName.Length - 2] != 'a'
                            && tableName[tableName.Length - 2] != 'e'
                            && tableName[tableName.Length - 2] != 'i'
                            && tableName[tableName.Length - 2] != 'o'
                            && tableName[tableName.Length - 2] != 'u')
                {
                    tableName = tableName.Substring(0, tableName.Length - 1) + "ies";
                }
                else
                {
                    tableName += "s";
                }
            }

            return tableName;
        }

        private static bool IsDeletable<T>()
        {
            bool deletable = false;
            object[] attrs = typeof(T).GetCustomAttributes(typeof(DBTable), false);
            if (attrs != null && attrs.Length > 0)
            {
                deletable = ((DBTable)attrs[0]).Deletable;
            }
            return deletable;
        }
        private static bool IsInsertable<T>()
        {
            bool insertable = false;
            object[] attrs = typeof(T).GetCustomAttributes(typeof(DBTable), false);
            if (attrs != null && attrs.Length > 0)
            {
                insertable = ((DBTable)attrs[0]).Insertable;
            }
            return insertable;
        }
        private static bool IsUpdateable<T>()
        {
            bool updateable = false;
            object[] attrs = typeof(T).GetCustomAttributes(typeof(DBTable), false);
            if (attrs != null && attrs.Length > 0)
            {
                updateable = ((DBTable)attrs[0]).Updateable;
            }
            return updateable;
        }

        private static List<PropertyNameValue> ExtractKeyValues<T>(T item, DBField.DBFieldKeys keyType)
        {
            List<PropertyNameValue> values = new List<PropertyNameValue>();

            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (props != null)
            {
                foreach (PropertyInfo prop in props)
                {
                    object[] attrs = prop.GetCustomAttributes(typeof(DBField), false);
                    if (attrs != null && attrs.Length > 0)
                    {
                        if ((((DBField)attrs[0]).Key & keyType) == keyType)
                        {
                            string fieldName = string.IsNullOrWhiteSpace(((DBField)attrs[0]).FieldName) ? prop.Name : ((DBField)attrs[0]).FieldName;
                            values.Add(new PropertyNameValue(fieldName, prop.Name, prop.GetValue(item, null)));
                        }
                    }
                }
            }
            if (values.Count == 0)
            {
                throw new KeyPropertyNotFoundException(keyType);
            }
            return values;
        }

        private static object GetPropertyValue<T>(T item, string propertyName)
        {
            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (props != null)
            {
                foreach (PropertyInfo prop in props)
                {
                    object[] attrs = prop.GetCustomAttributes(typeof(DBField), false);
                    if (attrs != null && attrs.Length > 0)
                    {
                        if (string.Equals(prop.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                        {
                            object obj = prop.GetValue(item, null);

                            if ((((DBField)attrs[0]).Key & DBField.DBFieldKeys.Nullable) == DBField.DBFieldKeys.Nullable && obj == null)
                            {
                                return DBNull.Value;
                            }
                            else
                            {
                                return obj;
                            }
                        }
                    }
                }
            }

            throw new DBFieldNotFoundException(propertyName);
        }

        private static string GetOrderByClause<T>(DAOrderByProperty[] orderByProperties)
        {
            string clause = @"";

            if (orderByProperties != null && orderByProperties.Length > 0)
            {
                foreach (DAOrderByProperty prop in orderByProperties)
                {
                    string safeColumnName = GetSafeColumnNameByProperty<T>(prop.PropertyName);
                    string safePropertyName = GetSafePropertyNameByProperty<T>(prop.PropertyName);

                    clause += (clause.Length > 0 ? " , " : " ") + (@" " + safeColumnName + " " + prop.OrderByType.ToString() + " ");
                }

                clause = " ORDER BY " + clause;
            }

            return clause;
        }

        private static string GetWhereClause<T>(DAMatchProperty[] matchProperties)
        {
            string clause = @"";

            foreach (DAMatchProperty prop in matchProperties)
            {
                string safeColumnName = GetSafeColumnNameByProperty<T>(prop.PropertyName);
                string safePropertyName = GetSafePropertyNameByProperty<T>(prop.PropertyName);

                clause += (clause.Length > 0 ? " AND " : " ") + (@" " + safeColumnName + " " + prop.Comparator + " @" + safePropertyName + " ");
            }

            if (string.IsNullOrWhiteSpace(clause))
            {
                throw new WhereClauseEmptyException();
            }

            return clause;
        }

        /// <summary>
        /// Generates the WHERE clause for a query using the property values passed in
        /// </summary>
        /// <param name="primaryKeyFields"></param>
        /// <returns></returns>
        private static string GetWhereClause(List<PropertyNameValue> primaryKeyFields)
        {
            string clause = "";

            foreach (PropertyNameValue pair in primaryKeyFields)
            {
                clause += " " + pair.ColumnName + "=@" + pair.PropertyName + " AND ";
            }

            if (primaryKeyFields.Count > 0)
            {
                clause = clause.Substring(0, clause.Length - 5);
            }
            if (String.IsNullOrWhiteSpace(clause))
            {
                throw new Exception("Where clause is empty.  Query would have unintended results.");
            }
            return clause;
        }

        /// <summary>
        /// Generates the SET clause using the property values passed in
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="updatableFields"></param>
        /// <returns></returns>
        private static string GetSetClause<T>(List<PropertyNameValue> updatableFields)
        {
            string clause = "";
            foreach (PropertyNameValue t in updatableFields)
            {
                clause += " " + t.ColumnName + "=@" + t.PropertyName + ",";
            }
            if (string.IsNullOrWhiteSpace(clause))
            {
                throw new SetClauseEmptyException();
            }
            return clause.Substring(0, clause.Length - 1);
        }

        private static List<string> GetKeyFields(Type type, DBField.DBFieldKeys keyType)
        {
            List<string> names = new List<string>();
            PropertyInfo[] props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (props != null)
            {
                foreach (PropertyInfo prop in props)
                {
                    object[] attrs = prop.GetCustomAttributes(typeof(DBField), false);
                    if (attrs != null && attrs.Length > 0)
                    {
                        if ((((DBField)attrs[0]).Key & keyType) == keyType)
                        {
                            names.Add(string.IsNullOrWhiteSpace(((DBField)attrs[0]).FieldName) ? prop.Name : ((DBField)attrs[0]).FieldName);
                        }
                    }
                }
            }
            if (names.Count == 0)
            {
                throw new KeyPropertyNotFoundException(keyType);
            }
            return names;
        }

        /// <summary>
        /// Loads an item of generic type T by using .NET reflection to infer which properties have 
        /// DB-aware attributes that map to specific columns in the table or result set.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dr"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        private static T Load<T>(SqlDataReader dr, T t)
        {
            t = Activator.CreateInstance<T>();
            PropertyInfo[] props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            if (props != null)
            {
                foreach (PropertyInfo prop in props)
                {
                    object[] attrs = prop.GetCustomAttributes(typeof(DBField), false);
                    if (attrs != null && attrs.Length > 0)
                    {
                        string fieldName = string.IsNullOrWhiteSpace(((DBField)attrs[0]).FieldName) ? prop.Name : ((DBField)attrs[0]).FieldName;

                        try
                        {
                            if ((((DBField)attrs[0]).Key & DBField.DBFieldKeys.Nullable) == DBField.DBFieldKeys.Nullable)
                            {
                                if (dr[fieldName] == DBNull.Value)
                                {
                                    prop.SetValue(t, null, null);
                                }
                                else
                                {
                                    // special handling of SqlDateTime
                                    if (prop.PropertyType == typeof(SqlDateTime) &&
                                        dr[fieldName].GetType() == typeof(DateTime))
                                    {
                                        SqlDateTime dateVal = new SqlDateTime((DateTime)dr[fieldName]);
                                        prop.SetValue(t, dateVal, null);
                                    }
                                    else
                                    {
                                        prop.SetValue(t, dr[fieldName], null);
                                    }
                                }
                            }
                            else
                            {
                                // special handling of SqlDateTime
                                if (dr[fieldName] != DBNull.Value &&
                                    prop.PropertyType == typeof(SqlDateTime) &&
                                    dr[fieldName].GetType() == typeof(DateTime))
                                {
                                    SqlDateTime dateVal = new SqlDateTime((DateTime)dr[fieldName]);
                                    prop.SetValue(t, dateVal, null);
                                }
                                else
                                {
                                    prop.SetValue(t, dr[fieldName], null);
                                }

                            }
                        }
                        catch (ArgumentException ex)
                        {
                            throw new ArgumentException(ex.Message + " (" + fieldName + ")", ex);
                        }
                    }
                }
            }

            return t;
        }



        #endregion

        /// <summary>
        /// Mapping between DB table name and object property name.
        /// obj represents the value of the mapping
        /// </summary>
        private class PropertyNameValue
        {
            public PropertyNameValue(string columnName, string propertyName, object obj)
            {
                this.columnName = columnName;
                this.propertyName = propertyName;
                this.obj = obj;
            }

            private string columnName;

            public string ColumnName
            {
                get { return columnName; }
                set { columnName = value; }
            }

            private string propertyName;

            public string PropertyName
            {
                get { return propertyName; }
                set { propertyName = value; }
            }

            private object obj;

            public object Obj
            {
                get { return obj; }
                set { obj = value; }
            }
        }

        #endregion
    }

    public class DataAccessUtil
    {
        private static bool useProcessContext = true;
        private static DataAccessUtilTestModes testing = DataAccessUtilTestModes.Production;

        /// <summary>
        /// Testing causes the DataAccessUtil to strip out all fully qualified table names to ensure that they run in the database 
        /// </summary>
        public static DataAccessUtilTestModes Mode
        {
            get { return DataAccessUtil.testing; }
            set { DataAccessUtil.testing = value; }
        }

        /// <summary>
        /// Using process context allows us to avoid the double-hop problem, since the underlying process's credentials
        /// will be passed when making the connection to the database instead of the passed credentials from a
        /// remote API call.  When DB access is necessary directly from the client (e.g. from an internal website), set
        /// this value to FALSE.
        /// </summary>
        public static bool UseProcessContext
        {
            get { return DataAccessUtil.useProcessContext; }
            set { DataAccessUtil.useProcessContext = value; }
        }

        /// <summary>
        /// Delegate for loading DTOs with data from a SqlReader
        /// </summary>
        /// <typeparam name="T">Any class derived from DtoBase</typeparam>
        /// <param name="dr">SqlDataReader instance</param>
        /// <returns></returns>
        public delegate T LoadMethod<T>(SqlDataReader dr, T t);

        /// <summary>
        /// Generic method with generic list of parameters
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public delegate object GenericMethod(params object[] parameters);

        /// <summary>
        /// Executes passed method without impersonation
        /// </summary>
        /// <param name="method"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public static object ExecMethodWithoutImpersonation(GenericMethod method, params object[] parameters)
        {
            WindowsImpersonationContext impersonationContext = DataAccessUtil.RevertToProcessContext();
            object returnValue = null;

            try
            {
                returnValue = method(parameters);
            }
            catch (Exception exception)
            {
                // revert impersonation here because finally block won't run since we're rethrowing the exception
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
                throw;
            }
            finally
            {
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
            }

            return returnValue;
        }

        /// <summary>
        /// Executes a stored procedure and returns a list of type T containing
        /// DTOs filled using the LoadMethod delegate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cmd"></param>
        /// <param name="loadMethod"></param>
        /// <returns></returns>
        public static List<T> ExecReaderLoadMultipleRecords<T>(SqlCommand cmd, LoadMethod<T> loadMethod)
        {
            List<T> results = DataAccessUtil.ExecReader<T>(cmd, loadMethod, default(T));
            return results;
        }

        /// <summary>
        /// Executes a stored procedure and returns the first instance of a list of result objects of type T
        /// If list is empty, returns default(T) or null.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cmd"></param>
        /// <param name="loadMethod"></param>
        /// <returns></returns>
        public static T ExecReaderLoadSingleRecord<T>(SqlCommand cmd, LoadMethod<T> loadMethod)
        {
            return DataAccessUtil.ExecReaderLoadSingleRecord<T>(cmd, loadMethod, default(T));
        }

        /// <summary>
        /// Executes a stored procedure and returns the first instance of a list of result objects of type T
        /// If list is empty, returns default(T) or null.
        /// Instance of T, t, is passed for use in the LoadMethod
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cmd"></param>
        /// <param name="loadMethod"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        public static T ExecReaderLoadSingleRecord<T>(SqlCommand cmd, LoadMethod<T> loadMethod, T t)
        {
            List<T> results = DataAccessUtil.ExecReader<T>(cmd, loadMethod, t);
            return results.Count > 0 ? results[0] : default(T);
        }

        /// <summary>
        /// Generic method that executes a stored procedure and fills the container object with the results.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cmd"></param>
        /// <param name="loadMethod"></param>
        /// <param name="container"></param>
        /// <param name="t"></param>
        private static List<T> ExecReader<T>(SqlCommand cmd, LoadMethod<T> loadMethod, T t)
        {
            ValidateCommand(cmd);

            List<T> results = new List<T>();

            WindowsImpersonationContext impersonationContext = DataAccessUtil.RevertToProcessContext();

            try
            {
                using (SqlConnection dbCon = (cmd.Connection == null ? DbConnection.Get() : cmd.Connection))
                {
                    dbCon.Open();
                    cmd.Connection = dbCon;

                    using (SqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            T item = loadMethod(dr, t);
                            results.Add(item);
                        }
                    }
                }
            }
            catch (Exception exception)
            {
                // revert impersonation here because finally block won't run since we're rethrowing the exception
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
                throw;
            }
            finally
            {
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
            }
            return results;
        }

        private static void ValidateCommand(SqlCommand cmd)
        {
            // TODO: add support for modifying database path so allow for automated testing
        }

        /// <summary>
        /// Executes a SqlCommand by calling its ExecuteNonQuery method.
        /// Abstracts away the SqlConnection code.
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        public static int ExecNonQuery(SqlCommand cmd)
        {
            ValidateCommand(cmd);
            int rowsAffected = 0;

            WindowsImpersonationContext impersonationContext = DataAccessUtil.RevertToProcessContext();

            try
            {
                using (SqlConnection dbCon = (cmd.Connection == null ? DbConnection.Get() : cmd.Connection))
                {
                    dbCon.Open();
                    cmd.Connection = dbCon;
                    rowsAffected = cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                // revert impersonation here because finally block won't run since we're rethrowing the exception
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
                throw;
            }
            finally
            {
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
            }

            return rowsAffected;
        }

        /// <summary>
        /// Executes a SqlCommand by calling its ExecuteScalar method.
        /// Abstracts away the SqlConnection code.
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        public static object ExecScalar(SqlCommand cmd)
        {
            ValidateCommand(cmd);
            object result = null;

            WindowsImpersonationContext impersonationContext = DataAccessUtil.RevertToProcessContext();

            try
            {
                using (SqlConnection dbCon = (cmd.Connection == null ? DbConnection.Get() : cmd.Connection))
                {
                    dbCon.Open();
                    cmd.Connection = dbCon;
                    result = cmd.ExecuteScalar();
                }
            }
            catch (Exception ex)
            {
                // revert impersonation here because finally block won't run since we're rethrowing the exception
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
                throw;
            }
            finally
            {
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
            }

            return result;
        }

        public static int ExecNonQueriesInTransaction(List<SqlCommand> commands)
        {
            if (commands == null)
            {
                throw new ArgumentNullException("commands", "Parameter commands is NULL.");
            }

            if (commands.Count == 0)
            {
                throw new ArgumentException("Parameter commands is empty.", "commands");
            }

            int rowsAffected = 0;

            WindowsImpersonationContext impersonationContext = DataAccessUtil.RevertToProcessContext();

            try
            {
                using (SqlConnection dbCon = (commands[0].Connection == null ? DbConnection.Get() : commands[0].Connection))
                {
                    dbCon.Open();
                    SqlTransaction tx = dbCon.BeginTransaction();

                    try
                    {
                        foreach (SqlCommand cmd in commands)
                        {
                            ValidateCommand(cmd);
                            cmd.Connection = dbCon;
                            cmd.Transaction = tx;
                            rowsAffected += cmd.ExecuteNonQuery();
                        }
                        tx.Commit();
                    }
                    catch (Exception exception)
                    {
                        try
                        {
                            tx.Rollback();
                        }
                        catch (Exception rollbackException)
                        {
                            throw;
                        }

                        throw;
                    }
                }
            }
            catch (Exception)
            {
                // revert impersonation here because finally block won't run since we're rethrowing the exception
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
                throw;
            }
            finally
            {
                DataAccessUtil.ReturnToImpersonationContext(impersonationContext);
            }

            return rowsAffected;

        }

        public static int ExecNonQueriesInTransaction(params SqlCommand[] commands)
        {
            return ExecNonQueriesInTransaction(new List<SqlCommand>(commands));
        }

        /// <summary>
        /// Builds a string representation of the contents of a SqlCommand object.
        /// Used primarily for debug and error logs.
        /// </summary>
        /// <param name="cmd"></param>
        /// <returns></returns>
        private static string SqlCommandToString(SqlCommand cmd)
        {
            StringBuilder paramStr = new StringBuilder(cmd.CommandText);
            paramStr.Append(", Parameters: ");
            foreach (SqlParameter parameter in cmd.Parameters)
            {
                paramStr.Append(parameter.ParameterName);
                paramStr.Append("=");
                paramStr.Append(parameter.Value != null ? parameter.Value.ToString() : "NULL");
                paramStr.Append("(");
                paramStr.Append(parameter.Value != null ? parameter.Value.GetType().ToString() : "NULL");
                paramStr.Append("); ");
            }
            return paramStr.ToString();
        }

        public static int LoadInt32(SqlDataReader dr, int int32)
        {
            return Convert.ToInt32(dr[0] == DBNull.Value ? default(int) : dr[0]);
        }

        public static long LoadInt64(SqlDataReader dr, long int64)
        {
            return Convert.ToInt64(dr[0] == DBNull.Value ? default(long) : dr[0]);
        }

        public static byte LoadByte(SqlDataReader dr, byte singleByte)
        {
            return Convert.ToByte(dr[0] == DBNull.Value ? default(byte) : dr[0]);
        }

        public static double LoadDouble(SqlDataReader dr, double theDouble)
        {
            return Convert.ToDouble(dr[0] == DBNull.Value ? default(double) : dr[0]);
        }

        public static decimal LoadDecimal(SqlDataReader dr, decimal theDecimal)
        {
            return Convert.ToDecimal(dr[0] == DBNull.Value ? default(decimal) : dr[0]);
        }

        public static string LoadString(SqlDataReader dr, string theString)
        {
            return Convert.ToString(dr[0] == DBNull.Value ? default(string) : dr[0]);
        }

        public static DataTable LoadDataTable(SqlDataReader dr, DataTable dataTable)
        {
            // setup table columns
            dataTable = new DataTable();
            for (int i = 0; i < dr.FieldCount; ++i)
            {
                string columnName = dr.GetName(i);
                Type type = dr.GetFieldType(i);
                dataTable.Columns.Add(columnName, type);
            }

            do
            {
                DataRow row = dataTable.NewRow();
                for (int i = 0; i < dr.FieldCount; ++i)
                {
                    row[i] = dr.GetValue(i);
                }
                dataTable.Rows.Add(row);
            }
            while (dr.Read());

            return dataTable;
        }

        public static DataSet LoadDataSet(SqlDataReader dr, DataSet dataSet)
        {
            // setup table columns
            dataSet = new DataSet();
            bool firstTable = true;

            do
            {
                DataTable table = new DataTable();

                if (!firstTable)
                {
                    // first table already had dr.Read() called
                    // subsequent tables need it called before passing to LoadDataTable
                    dr.Read();
                }

                table = DataAccessUtil.LoadDataTable(dr, table);

                if (table != null)
                {
                    dataSet.Tables.Add(table);
                }

                firstTable = false;
            }
            while (dr.NextResult());

            return dataSet;
        }

        public static object GetDateTimeOrDBNull(DateTime? date)
        {
            if (date != null && date.HasValue)
            {
                return date.Value;
            }
            else
            {
                return DBNull.Value;
            }
        }

        public static object GetDateTimeOrDBNull(DateTime date)
        {
            if (date.Date <= DateTime.MinValue.Date || date.Date <= SqlDateTime.MinValue.Value.Date)
            {
                return DBNull.Value;
            }
            else
            {
                return date;
            }
        }

        public static DateTime? LoadNullableDateTime(SqlDataReader dr, string columnName)
        {
            if (dr[columnName] == DBNull.Value)
            {
                return null;
            }
            else
            {
                return new DateTime?((DateTime)dr[columnName]);
            }
        }

        public static void LoadProperty<T, V>(SqlDataReader dr, T obj, string propertyName)
        {
            obj.GetType().InvokeMember(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.SetProperty, Type.DefaultBinder, obj, new object[] { (V)dr[propertyName] });
        }

        public static Type ConvertTypeNameToType(string typeName)
        {
            return DAUtil.ConvertTypeNameToType(typeName);
        }

        public static bool TryLoadProperty<T, V>(SqlDataReader dr, T obj, string propertyName)
        {
            try
            {
                LoadProperty<T, V>(dr, obj, propertyName);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static DateTime ToSmallDateTime(DateTime date)
        {
            if (date < new DateTime(1900, 1, 1))
            {
                date = new DateTime(1900, 1, 1);
            }
            else if (date > new DateTime(2079, 06, 06, 23, 59, 0))
            {
                date = new DateTime(2079, 06, 06, 23, 59, 0);
            }

            return date;
        }

        /// <summary>
        /// Reverts the identity context to the original process context
        /// e.g. DOMAIN\username under which the process started.
        /// </summary>
        /// <returns></returns>
        public static WindowsImpersonationContext RevertToProcessContext()
        {
            if (useProcessContext)
            {
                // revert to original process identity
                return WindowsIdentity.Impersonate(IntPtr.Zero);
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// If impersonation was occuring before RevertToProcessContext() was called, reverts back to impersonation context.
        /// </summary>
        /// <param name="context"></param>
        public static void ReturnToImpersonationContext(WindowsImpersonationContext context)
        {
            // revert to impersonation identity if exists
            if (context != null)
            {
                context.Undo();
            }
        }

    }
}