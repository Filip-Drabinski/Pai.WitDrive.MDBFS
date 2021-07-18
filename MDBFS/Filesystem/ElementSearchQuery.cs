using System;
using System.Collections.Generic;

namespace MDBFS.Filesystem
{
    public enum ESearchCondition
    {
        Eq,
        Ne,
        Lt,
        Lte,
        Gt,
        Gte,

        //Reqex,
        Contains
    }

    public class ElementSearchQuery
    {
        public ElementSearchQuery()
        {
            Metadata = new List<(string fieldName, ESearchCondition condition, object value)>();
            CustomMetadata = new List<(string fieldName, ESearchCondition condition, object value)>();
        }

        public string Id { get; set; }
        public string ParentId { get; set; }
        public List<(ESearchCondition condition, string value)> Name { get; set; }
        public List<(ESearchCondition condition, DateTime value)> Opened { get; set; }
        public List<(ESearchCondition condition, DateTime value)> Created { get; set; }
        public List<(ESearchCondition condition, DateTime value)> Modified { get; set; }
        public List<(ESearchCondition condition, bool value)> Removed { get; set; }
        public List<(string fieldName, ESearchCondition condition, object value)> Metadata { get; set; }
        public List<(string fieldName, ESearchCondition condition, object value)> CustomMetadata { get; set; }
    }
}