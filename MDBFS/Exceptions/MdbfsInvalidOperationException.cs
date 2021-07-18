using System;
using System.Runtime.Serialization;

namespace MDBFS.Exceptions
{
    [Serializable]
    public class MdbfsInvalidOperationException : Exception
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public MdbfsInvalidOperationException()
        {
        }

        public MdbfsInvalidOperationException(string message) : base(message)
        {
        }

        public MdbfsInvalidOperationException(string message, Exception inner) : base(message, inner)
        {
        }

        protected MdbfsInvalidOperationException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}