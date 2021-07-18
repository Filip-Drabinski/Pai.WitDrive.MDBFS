using System;

namespace MDBFS.Exceptions
{
    [Serializable]
    public class MbdfsNotDirectoryException : Exception
    {
        public MbdfsNotDirectoryException() { }
        public MbdfsNotDirectoryException(string message) : base(message) { }
        public MbdfsNotDirectoryException(string message, Exception inner) : base(message, inner) { }
        protected MbdfsNotDirectoryException(
          System.Runtime.Serialization.SerializationInfo info,
          System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
    }
}
