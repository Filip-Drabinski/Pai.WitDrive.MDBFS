using System;
using System.Runtime.Serialization;

namespace MDBFS.Exceptions
{
    [Serializable]
    public class MdbfsGroupNotFoundException : Exception
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public MdbfsGroupNotFoundException()
        {
        }

        public MdbfsGroupNotFoundException(string message) : base(message)
        {
        }

        public MdbfsGroupNotFoundException(string message, Exception inner) : base(message, inner)
        {
        }

        protected MdbfsGroupNotFoundException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}