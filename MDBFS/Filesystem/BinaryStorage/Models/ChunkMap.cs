using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MDBFS.FileSystem.BinaryStorage.Models
{
    public class ChunkMap
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        public long Length { get; set; }
        public List<string> ChunksIDs { get; set; }
        public bool Removed { get; set; }

        public ChunkMap()
        {
            Removed = true;
        }
    }
}