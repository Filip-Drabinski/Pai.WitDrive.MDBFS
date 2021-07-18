using MDBFS.Filesystem.AccessControl;
using MDBFS.Filesystem.Models;
using MongoDB.Driver;

namespace MDBFS.Filesystem
{
    public class FileSystemClient
    {
        public FileSystemClient(IMongoDatabase database, int chunkSize = 1048576)
        {
            IMongoCollection<Element> elements =
                database.GetCollection<Element>(nameof(MDBFS) + '.' + nameof(Filesystem) + '.' + nameof(elements));
            Files = new Files(elements, chunkSize);
            Directories = new Directories(elements, Files);
            AccessControl = new AccessControlClient(database, elements, Files, Directories);
        }

        public Files Files { get; }
        public Directories Directories { get; }
        public AccessControlClient AccessControl { get; }
    }
}