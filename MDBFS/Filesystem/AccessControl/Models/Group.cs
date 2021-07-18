using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace MDBFS.Filesystem.AccessControl.Models
{
    public class Group
    {
        protected string Id { get; set; }

        public List<string> Members { get; set; }

        [BsonId]
        public string Name
        {
            get => Id;
            set => Id = value;
        }
    }
}