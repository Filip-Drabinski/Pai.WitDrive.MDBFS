using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MDBFS.FileSystem.BinaryStorage.Models
{
    public class Chunk
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        public byte[] Bytes { get; set; }
    }
}