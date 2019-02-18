namespace Sitecore.Support.ContentSearch.Azure
{
    using System;
    using System.Text;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Sitecore.ContentSearch.Azure;
    using Newtonsoft.Json;
    using Sitecore.ContentSearch.Diagnostics;

    public class CloudBatchBuilder : ICloudBatchBuilder
    {
        //https://docs.microsoft.com/en-us/azure/search/search-limits-quotas-capacity
        //Maximum search term size is 32,766 bytes (32 KB minus 2 bytes) of UTF-8 encoded text
        public const int AzureSearchMaxTermSize = 32766;

        public const string AzureSearchLimitationsDoc =
            @"https://docs.microsoft.com/en-us/azure/search/search-limits-quotas-capacity";

        private readonly List<CloudSearchDocument> documents;

        public CloudBatchBuilder()
        {
            this.documents = new List<CloudSearchDocument>();
        }

        public int MaxDocuments { get; set; }

        public bool IsFull => this.documents.Count >= this.MaxDocuments;

        public void AddDocument(CloudSearchDocument document)
        {
            if (this.documents.Count > this.MaxDocuments)
            {
                throw new ArgumentOutOfRangeException(string.Format("Number of documents in the batch exceeded {0}", this.MaxDocuments));
            }

            this.documents.Add(document);
        }

        public void Clear()
        {
            this.documents.Clear();
        }

        public ICloudBatch Release()
        {
            var batchItems = new List<Dictionary<string, object>>();

            foreach (var document in this.documents)
            {
                var dictionary = new Dictionary<string, object>(document.Fields.Count);
                #region fix for #176528
                foreach (var field in document.Fields.OrderBy(field => field.Key))
                {
                    var fieldValue = GetValue(field.Value);
                    if (!HasValue(fieldValue))
                    {
                        continue;
                    }
                    string json = JsonConvert.SerializeObject(
                        new Dictionary<string, object> {{"value", fieldValue}}, Formatting.Indented,
                        new JsonSerializerSettings {DateFormatString = "yyyy-MM-ddTHH:mm:ss.fffZ"});
                    if (Encoding.UTF8.GetByteCount(json) > AzureSearchMaxTermSize)
                    {
                        object uniqueid;
                        //we do not have acess to field name translator here. Let's assume that "_uniqueid" is mapped to "uniqueid_1" cloud field name, which is default.
                        var uniqueidString = document.Fields.TryGetValue("uniqueid_1", out uniqueid)
                            ? uniqueid.ToString()
                            : "UNKNOWN";
                        CrawlingLog.Log.Warn(string.Format("Length of the '{0}' field is {1} which exceeds maximum allowed length {2}. Document: {3}. See {4} for details.", field.Key, json.Length, AzureSearchMaxTermSize, uniqueidString, AzureSearchLimitationsDoc));
                        continue;
                    }
                    dictionary.Add(field.Key, fieldValue);
                }
                //var dictionary = document.Fields.Where(field => HasValue(GetValue(field.Value))).
                //        OrderBy(field => field.Key).ToDictionary(pair => pair.Key, pair => GetValue(pair.Value));
                #endregion

                var action = char.ToLower(document.Action.ToString()[0]) + document.Action.ToString().Substring(1);

                dictionary.Add("@search.action", action);

                batchItems.Add(dictionary);
            }

            return new CloudBatch(batchItems);
        }

        public IEnumerator<CloudSearchDocument> GetEnumerator()
        {
            return this.documents.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        private static bool HasValue(object item)
        {
            if (item == null)
            {
                return false;
            }

            var objects = item as IEnumerable<object>;
            if (objects != null)
            {
                return objects.Any();
            }

            return true;
        }

        private static object GetValue(object item)
        {
            var objects = item as IEnumerable<object>;
            if (objects != null)
            {
                return objects.Where(@object => @object != null);
            }

            var dictionary = item as Dictionary<string, string>;
            if (dictionary != null)
            {
                return dictionary.Where(pair => pair.Value != null).ToDictionary(pair => pair.Key, pair => pair.Value);
            }

            return item;
        }
    }
}