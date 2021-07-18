using System.Collections.Generic;
using System.Threading.Tasks;

namespace MDBFS.Misc
{
    public class NamedReaderWriterLock
    {
        private readonly Dictionary<string, CustomReaderWriterLock> _rwls;

        public NamedReaderWriterLock()
        {
            _rwls = new Dictionary<string, CustomReaderWriterLock>();
        }

        public string AcquireReaderLock(string name)
        {
            CustomReaderWriterLock crwl;
            lock (this)
            {
                if (_rwls.ContainsKey(name))
                {
                    crwl = _rwls[name];
                }
                else
                {
                    crwl = new CustomReaderWriterLock();
                    _rwls[name] = crwl;
                }
            }

            return crwl.AcquireReaderLock();
        }

        public string AcquireWriterLock(string name)
        {
            CustomReaderWriterLock crwl;
            lock (this)
            {
                if (_rwls.ContainsKey(name))
                {
                    crwl = _rwls[name];
                }
                else
                {
                    crwl = new CustomReaderWriterLock();
                    _rwls[name] = crwl;
                }
            }

            return crwl.AcquireWriterLock();
        }

        public void ReleaseLock(string name, string id)
        {
            CustomReaderWriterLock crwl;
            lock (this)
            {
                crwl = _rwls[name];
                if (crwl.Counter - 1 <= 0) _rwls.Remove(name);
            }

            crwl.ReleaseLock(id);
        }

        public async Task<string> AcquireReaderLockAsync(string name)
        {
            CustomReaderWriterLock crwl;
            lock (this)
            {
                if (_rwls.ContainsKey(name))
                {
                    crwl = _rwls[name];
                }
                else
                {
                    crwl = new CustomReaderWriterLock();
                    _rwls[name] = crwl;
                }
            }

            return await crwl.AcquireReaderLockAsync();
        }

        public async Task<string> AcquireWriterLockAsync(string name)
        {
            CustomReaderWriterLock crwl;
            lock (this)
            {
                if (_rwls.ContainsKey(name))
                {
                    crwl = _rwls[name];
                }
                else
                {
                    crwl = new CustomReaderWriterLock();
                    _rwls[name] = crwl;
                }
            }

            return await crwl.AcquireWriterLockAsync();
        }

        public async Task ReleaseLockAsync(string name, string id)
        {
            CustomReaderWriterLock crwl;
            lock (this)
            {
                crwl = _rwls[name];
                if (crwl.Counter - 1 <= 0) _rwls.Remove(name);
            }

            await crwl.ReleaseLockAsync(id);
        }
    }
}