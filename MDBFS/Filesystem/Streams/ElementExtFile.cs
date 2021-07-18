using MDBFS.Filesystem.Models;
using MDBFS.Misc;

namespace MDBFS.Filesystem.Streams
{
    internal static class ElementExtFile
    {
        internal static void IncreaseLength(this Element elem, long count)
        {
            if (!elem.Metadata.ContainsKey(nameof(EMetadataKeys.Length)))
                elem.Metadata[nameof(EMetadataKeys.Length)] = count;
            else
                elem.Metadata[nameof(EMetadataKeys.Length)] =
                    (long) elem.Metadata[nameof(EMetadataKeys.Length)] + count;
        }

        internal static void IncreaseLength(this Element elem, int count)
        {
            if (!elem.Metadata.ContainsKey(nameof(EMetadataKeys.Length)))
                elem.Metadata[nameof(EMetadataKeys.Length)] = (long) count;
            else
                elem.Metadata[nameof(EMetadataKeys.Length)] =
                    (long) elem.Metadata[nameof(EMetadataKeys.Length)] + count;
        }

        internal static long GetLength(this Element elem)
        {
            if (!elem.Metadata.ContainsKey(nameof(EMetadataKeys.Length)))
                return 0L;
            return (long) elem.Metadata[nameof(EMetadataKeys.Length)];
        }
    }
}