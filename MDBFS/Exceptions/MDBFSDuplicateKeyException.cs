using System;

namespace MDBFS.Exceptions
{

    [Serializable]
    public class MdbfsDuplicateKeyException : Exception
    {
        public MdbfsDuplicateKeyException() { }
        public MdbfsDuplicateKeyException(string message) : base(message) { }
        public MdbfsDuplicateKeyException(string message, Exception inner) : base(message, inner) { }
        protected MdbfsDuplicateKeyException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
