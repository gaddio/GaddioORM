using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Gaddio.ORM
{
    [Serializable]
    public class DBTableNotInserableException : Exception
    {
        public DBTableNotInserableException()
            : base("Object is not marked as Insertable")
        {

        }
        protected DBTableNotInserableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }
    [Serializable]
    public class DBTableNotUpdatableException : Exception
    {
        public DBTableNotUpdatableException()
            : base("Object is not marked as Updateable")
        {

        }
        protected DBTableNotUpdatableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }
    [Serializable]
    public class DBTableNotDeletableException : Exception
    {
        public DBTableNotDeletableException()
            : base("Object is not marked as Deleteable")
        {

        }
        protected DBTableNotDeletableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }
    [Serializable]
    public class DBFieldNotFoundException : Exception
    {
        public DBFieldNotFoundException(string propertyName)
            : base("DBField not found for property: " + propertyName)
        {
            this.propertyName = propertyName;
        }

        protected DBFieldNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            this.propertyName = (string)info.GetString("propertyName");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info != null)
            {
                info.AddValue("propertyName", this.propertyName);
            }
            base.GetObjectData(info, context);
        }

        private string propertyName = "";

        public string PropertyName
        {
            get { return propertyName; }
            set { propertyName = value; }
        }
    }

    [Serializable]
    public class PrimaryKeyPropertiesNotFoundException : Exception
    {
        public PrimaryKeyPropertiesNotFoundException()
            : base("Primary key properties not found")
        {
        }

        protected PrimaryKeyPropertiesNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }

    [Serializable]
    public class KeyPropertyNotFoundException : Exception
    {
        private DBField.DBFieldKeys propertyType = DBField.DBFieldKeys.None;

        public DBField.DBFieldKeys PropertyType
        {
            get { return propertyType; }
        }

        public KeyPropertyNotFoundException(DBField.DBFieldKeys propertyType)
            : base("Key property not found")
        {
            this.propertyType = propertyType;
        }

        protected KeyPropertyNotFoundException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }

    [Serializable]
    public class ParameterListEmptyException : Exception
    {
        public ParameterListEmptyException()
        {

        }

        protected ParameterListEmptyException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }

    [Serializable]
    public class WhereClauseEmptyException : Exception
    {
        public WhereClauseEmptyException()
        {

        }

        protected WhereClauseEmptyException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }

    [Serializable]
    public class SetClauseEmptyException : Exception
    {
        public SetClauseEmptyException()
        {

        }

        protected SetClauseEmptyException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
        }
    }
}