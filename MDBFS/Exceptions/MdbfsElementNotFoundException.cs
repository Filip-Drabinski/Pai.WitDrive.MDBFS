using System;

namespace MDBFS.Exceptions
{
    [Serializable]
    public class MdbfsElementNotFoundException : Exception
    {
        public MdbfsElementNotFoundException() { }
        public MdbfsElementNotFoundException(string message) : base(message) { }
        public MdbfsElementNotFoundException(string message, Exception inner) : base(message, inner) { }
        protected MdbfsElementNotFoundException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
