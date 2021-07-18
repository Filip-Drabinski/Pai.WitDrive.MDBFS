using System.Collections.Generic;
using MongoDB.Bson.Serialization.Attributes;

namespace MDBFS.Filesystem.AccessControl.Models
{
    public enum EUserRole
    {
        User,
        Admin
    }

    public class User
    {
        protected string Id { get; set; }

        [BsonId]
        public string Username
        {
            get => Id;
            set => Id = value;
        }

        public string RootDirectory { get; set; }
        public EUserRole Role { get; set; }
        public List<string> MemberOf { get; set; }
        public List<string> Permissions { get; set; }
    }
}