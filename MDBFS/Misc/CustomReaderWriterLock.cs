using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace MDBFS.Misc
{
    public class CustomReaderWriterLock
    {
        private class Elem
        {
            private static readonly List<string> ActiveIDs = new List<string>();

            public readonly string Id;
            public readonly SemaphoreSlim SemSl;
            public readonly bool Write;

            public Elem(bool write)
            {
                Id = GenerateId();
                SemSl = new SemaphoreSlim(0, 1);
                Write = write;
            }

            ~Elem()
            {
                RemoveId(Id);
            }

            private static string GenerateId()
            {
                string res;
                lock (ActiveIDs)
                {
                    do
                    {
                        res = Guid.NewGuid().ToString();
                    } while (ActiveIDs.Contains(res));

                    ActiveIDs.Add(res);
                }

                return res;
            }

            private static void RemoveId(string id)
            {
                lock (ActiveIDs)
                {
                    ActiveIDs.Remove(id);
                }
            }
        }

        private readonly SemaphoreSlim _semSl;
        private readonly Queue<Elem> _queue;
        private readonly Dictionary<string, Elem> _active;

        public long Counter
        {
            get
            {
                lock (this)
                {
                    return _queue.Count + (long) _active.Count;
                }
            }
        }

        public CustomReaderWriterLock()
        {
            _semSl = new SemaphoreSlim(1, 1);
            _queue = new Queue<Elem>();
            _active = new Dictionary<string, Elem>();
        }

        public string AcquireReaderLock()
        {
            return EnqueueElement(false);
        }

        public string AcquireWriterLock()
        {
            return EnqueueElement(true);
        }

        public async Task<string> AcquireReaderLockAsync()
        {
            return await EnqueueElementAsync(false);
        }

        public async Task<string> AcquireWriterLockAsync()
        {
            return await EnqueueElementAsync(true);
        }

        public void ReleaseLock(string id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            _semSl.Wait();
            if (!_active.ContainsKey(id)) throw new Exception("Unknown ID");
            _active.Remove(id);
            if (_queue.Count > 0)
            {
                var elem = _queue.Dequeue();
                _active[elem.Id] = elem;
                if (!elem.Write)
                    while (_queue.Count > 0 && !_queue.Peek().Write)
                    {
                        var elem2 = _queue.Dequeue();
                        _active[elem2.Id] = elem2;
                        elem2.SemSl.Release();
                    }

                elem.SemSl.Release();
            }

            _semSl.Release();
        }

        private string EnqueueElement(bool write)
        {
            var elem = new Elem(write);
            _semSl.Wait();
            if (_active.Count > 0)
            {
                _queue.Enqueue(elem);
            }
            else
            {
                _active[elem.Id] = elem;
                elem.SemSl.Release();
            }

            _semSl.Release();

            elem.SemSl.Wait();
            return elem.Id;
        }


        public async Task ReleaseLockAsync(string id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            await _semSl.WaitAsync();
            if (!_active.ContainsKey(id)) throw new Exception("Unknown ID");
            _active.Remove(id);
            if (_queue.Count > 0)
            {
                var elem = _queue.Dequeue();
                _active[elem.Id] = elem;
                if (!elem.Write)
                    while (_queue.Count > 0 && !_queue.Peek().Write)
                    {
                        var elem2 = _queue.Dequeue();
                        _active[elem2.Id] = elem2;
                        elem2.SemSl.Release();
                    }

                elem.SemSl.Release();
            }

            _semSl.Release();
        }

        private async Task<string> EnqueueElementAsync(bool write)
        {
            var elem = new Elem(write);
            await _semSl.WaitAsync();
            if (_active.Count > 0)
            {
                _queue.Enqueue(elem);
            }
            else
            {
                _active[elem.Id] = elem;
                elem.SemSl.Release();
            }

            _semSl.Release();

            await elem.SemSl.WaitAsync();
            return elem.Id;
        }
    }
}