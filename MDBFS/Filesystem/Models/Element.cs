using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MDBFS.Filesystem.Models
{
    public class Element
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; }

        [BsonRepresentation(BsonType.ObjectId)]
        public string ParentId { get; set; }

        public byte Type { get; set; }
        public string Name { get; set; }
        public DateTime Created { get; set; }
        public DateTime Modified { get; set; }
        public DateTime Opened { get; set; }
        public bool Removed { get; set; }
        public Dictionary<string, object> Metadata { get; set; }
        public Dictionary<string, object> CustomMetadata { get; set; }

        public Element()
        {
            Metadata = new Dictionary<string, object>();
            CustomMetadata = new Dictionary<string, object>();
        }

        public static Element Create(string id, string parentId, byte type, string name,
            Dictionary<string, object> metadata, Dictionary<string, object> customMetadata)
        {
            var elem = new Element
            {
                Id = id,
                ParentId = parentId,
                Type = type,
                Removed = false
            };
            elem.Opened = elem.Modified = elem.Created = DateTime.Now;
            if (metadata != null) elem.Metadata = metadata;
            if (customMetadata != null) elem.CustomMetadata = customMetadata;

            return elem;
        }
    }
}