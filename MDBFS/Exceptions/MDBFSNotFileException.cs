using System;

namespace MDBFS.Exceptions
{
    [Serializable]
    public class MdbfsNotFileException : Exception
    {
        public MdbfsNotFileException() { }
        public MdbfsNotFileException(string message) : base(message) { }
        public MdbfsNotFileException(string message, Exception inner) : base(message, inner) { }
        protected MdbfsNotFileException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
