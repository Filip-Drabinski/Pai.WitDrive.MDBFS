using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MDBFS.FileSystem.BinaryStorage.Models;
using MDBFS.Misc;
using MongoDB.Driver;

namespace MDBFS.FileSystem.BinaryStorage.Streams
{
    public class BinaryDownloadStream : Stream
    {
        public override long Position
        {
            get => _curretIndex;
            set => Seek(value, SeekOrigin.Begin);
        }

        public override long Length => _map.Length;
        public override bool CanWrite => false;
        public override bool CanTimeout => false;
        public override bool CanSeek => true;
        public override bool CanRead => false;

        private readonly NamedReaderWriterLock _nrwl;
        private string _nrwlId; //todo: add to constructor
        private readonly IMongoCollection<Chunk> _chunks;
        private readonly IMongoCollection<ChunkMap> _maps;
        private ChunkMap _map;

        private byte[] _innerBuffer;
        private long _curretIndex;
        private readonly long _maxChunkLenght;
        private long _currentChunkIndex;


        protected BinaryDownloadStream(IMongoCollection<Chunk> chunks, NamedReaderWriterLock namedReaderWriterLock,
            int maxChunkLenght, IMongoCollection<ChunkMap> maps)
        {
            _chunks = chunks;
            _innerBuffer = null;
            _nrwl = namedReaderWriterLock;
            _maxChunkLenght = maxChunkLenght;
            _maps = maps;
            _currentChunkIndex = 0;
            _curretIndex = 0;
        }

        public static (bool success, BinaryDownloadStream stream) Open(IMongoCollection<ChunkMap> maps,
            IMongoCollection<Chunk> chunks, int maxChunkLenght, string id, NamedReaderWriterLock namedReaderWriterLock)
        {
            var (success, map) = LoadElement(maps, id, namedReaderWriterLock);
            if (!success) return (false, null);

            var stream = new BinaryDownloadStream(chunks, namedReaderWriterLock, maxChunkLenght, maps)
            {
                _map = map
            };
            stream.Seek(0, SeekOrigin.Begin);


            stream._nrwlId = namedReaderWriterLock.AcquireReaderLock($"{nameof(Chunk)}.{map.Id}");
            return (true, stream);
        }

        public static async Task<(bool success, BinaryDownloadStream stream)> OpenAsync(IMongoCollection<ChunkMap> maps,
            IMongoCollection<Chunk> chunks, int maxChunkLenght, string id, NamedReaderWriterLock namedReaderWriterLock)
        {
            var (success, map) = await LoadElementAsync(maps, id, namedReaderWriterLock);
            if (!success) return (false, null);

            var stream = new BinaryDownloadStream(chunks, namedReaderWriterLock, maxChunkLenght, maps)
            {
                _map = map
            };
            await stream.SeekAsync(0, SeekOrigin.Begin);
            stream._nrwlId = await namedReaderWriterLock.AcquireReaderLockAsync($"{nameof(Chunk)}.{map.Id}");
            return (true, stream);
        }

        protected static (bool success, ChunkMap map) LoadElement(IMongoCollection<ChunkMap> maps, string id,
            NamedReaderWriterLock nrwl)
        {
            var lId = nrwl.AcquireReaderLock($"{nameof(ChunkMap)}.{id}");
            var mapSearch = maps.Find(x => x.Id == id).ToList();
            if (mapSearch.Count == 0) return (false, null);
            nrwl.ReleaseLock($"{nameof(ChunkMap)}.{id}", lId);
            return (true, mapSearch.First());
        }

        protected static async Task<(bool success, ChunkMap map)> LoadElementAsync(IMongoCollection<ChunkMap> maps,
            string id, NamedReaderWriterLock nrwl)
        {
            var lId = await nrwl.AcquireReaderLockAsync($"{nameof(ChunkMap)}.{id}");
            var mapSearch = (await maps.FindAsync(x => x.Id == id)).ToList();
            if (mapSearch.Count == 0) return (false, null);
            await nrwl.ReleaseLockAsync($"{nameof(ChunkMap)}.{id}", lId);
            return (true, mapSearch.First());
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_curretIndex >= _map.Length) return 0;
            if (offset + count > buffer.Length) throw new IndexOutOfRangeException();
            var buffIt = offset;
            if (count < _innerBuffer.Length - _currentChunkIndex)
            {
                Array.Copy(_innerBuffer, _currentChunkIndex, buffer, buffIt, count);
                _curretIndex += count;
                _currentChunkIndex += count;
                return count;
            }

            if (count == _innerBuffer.Length - _currentChunkIndex)
            {
                Array.Copy(_innerBuffer, _currentChunkIndex, buffer, buffIt, count);
                _curretIndex += count;
                _currentChunkIndex += count;
                _curretIndex = Seek(_curretIndex + 1, SeekOrigin.Begin);
                return count;
            }

            var rSum = 0;
            var countCpy = count;
            while (countCpy > 0)
            {
                var rCount = (int) Math.Min(_innerBuffer.Length - _currentChunkIndex, countCpy);
                if (rCount == 0) return rSum;
                Array.Copy(_innerBuffer, _currentChunkIndex, buffer, buffIt, rCount);
                countCpy -= rCount;
                _curretIndex += rCount - 1;
                buffIt += rCount;
                rSum += rCount;
                _curretIndex = Seek(_curretIndex + 1, SeekOrigin.Begin);
            }

            return rSum;
        }

        public new async Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            if (_curretIndex >= _map.Length) return 0;
            if (offset + count > buffer.Length) throw new IndexOutOfRangeException(nameof(count));
            var buffIt = offset;
            if (count < _innerBuffer.Length - _currentChunkIndex)
            {
                Array.Copy(_innerBuffer, _currentChunkIndex, buffer, buffIt, count);
                _curretIndex += count;
                return count;
            }

            if (count == _innerBuffer.Length - _currentChunkIndex)
            {
                Array.Copy(_innerBuffer, _currentChunkIndex, buffer, buffIt, count);
                _curretIndex += count;
                _curretIndex = await SeekAsync(_curretIndex + 1, SeekOrigin.Begin);
                return count;
            }

            var rSum = 0;
            var countCpy = count;
            while (countCpy > 0)
            {
                var rCount = (int) Math.Min(_innerBuffer.Length - _currentChunkIndex, countCpy);
                if (rCount == 0) return rSum;
                Array.Copy(_innerBuffer, _currentChunkIndex, buffer, buffIt, rCount);
                countCpy -= rCount;
                _curretIndex += rCount - 1;
                buffIt += rCount;
                rSum += rCount;
                _curretIndex = await SeekAsync(_curretIndex + 1, SeekOrigin.Begin);
            }

            return rSum;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            var nIndex = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _curretIndex + offset,
                SeekOrigin.End => _map.Length + offset,
                _ => throw new Exception("error")
            };

            if (nIndex > _map.Length) nIndex = _map.Length - 1;
            var (nChunk, nChunkIndex) = TranslateIndexToChunkIndex(nIndex);
            var chunkId = _map.ChunksIDs[(int) nChunk];

            var chunkSearch = _chunks.Find(x => x.Id == chunkId).ToList();
            if (chunkSearch.Count == 0) throw new Exception("error");
            _innerBuffer = chunkSearch.First().Bytes;


            _currentChunkIndex = nChunkIndex;
            _curretIndex = nIndex;
            return _curretIndex;
        }

        public async Task<long> SeekAsync(long offset, SeekOrigin origin)
        {
            var nIndex = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _curretIndex + offset,
                SeekOrigin.End => _map.Length + offset,
                _ => throw new Exception("error")
            };
            if (nIndex > _map.Length) nIndex = _map.Length - 1;
            var (nChunk, nChunkIndex) = TranslateIndexToChunkIndex(nIndex);
            var chunkId = _map.ChunksIDs[(int) nChunk];

            var chunkSearch = (await _chunks.FindAsync(x => x.Id == chunkId)).ToList();
            if (chunkSearch.Count == 0) throw new Exception("error");
            _innerBuffer = chunkSearch.First().Bytes;


            _currentChunkIndex = nChunkIndex;
            _curretIndex = nIndex;
            return _curretIndex;
        }


        public override void Flush()
        {
            if (_innerBuffer != null)
            {
                _innerBuffer = null;
                var lId = _nrwl.AcquireWriterLock($"{nameof(ChunkMap)}.{_map.Id}");
                var search = _maps.Find(x => x.Id == _map.Id);
                var lSearch = search.ToList();
                if (lSearch.Any())
                {
                    var m = search.First();
                    if (m.Removed)
                    {
                        _chunks.DeleteMany(x => m.ChunksIDs.Contains(x.Id));
                        _maps.DeleteOne(x => x.Id == _map.Id);
                    }
                }


                _nrwl.ReleaseLock($"{nameof(ChunkMap)}.{_map.Id}", lId);

                _nrwl.ReleaseLock($"{nameof(Chunk)}.{_map.Id}", _nrwlId);
            }
        }

        public new async Task FlushAsync()
        {
            if (_innerBuffer != null)
            {
                _innerBuffer = null;
                var lId = await _nrwl.AcquireWriterLockAsync($"{nameof(ChunkMap)}.{_map.Id}");
                var search = await _maps.FindAsync(x => x.Id == _map.Id);
                var lSearch = search.ToList();
                if (lSearch.Any())
                {
                    var m = search.First();
                    if (m.Removed)
                    {
                        await _chunks.DeleteManyAsync(x => m.ChunksIDs.Contains(x.Id));
                        await _maps.DeleteOneAsync(x => x.Id == _map.Id);
                    }
                }


                await _nrwl.ReleaseLockAsync($"{nameof(ChunkMap)}.{_map.Id}", lId);

                await _nrwl.ReleaseLockAsync($"{nameof(Chunk)}.{_map.Id}", _nrwlId);
            }
        }


        public override void Close()
        {
            Flush();
        }

        public new void Dispose()
        {
            Flush();
        }

        private (long ChunkID, long ChunkIndex) TranslateIndexToChunkIndex(long index)
        {
            if (index == 0) return (0, 0);
            var chunkId = (long) MathF.Floor(index / (float) _maxChunkLenght);
            var chunkIndex = index - chunkId * _maxChunkLenght;
            return (chunkId, chunkIndex);
        }


        #region NotSupported

#pragma warning disable IDE0060
#pragma warning disable CS8632
        public override int ReadTimeout
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int WriteTimeout
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        //public new static Stream Synchronized(Stream stream)
        //{
        //    throw new NotSupportedException();
        //}

        //public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback,
        //    object? state)
        //{
        //    throw new NotSupportedException();
        //}

        //public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback,
        //    object? state)
        //{
        //    throw new NotSupportedException();
        //}

        //public override void CopyTo(Stream destination, int bufferSize)
        //{
        //    throw new NotSupportedException();
        //}

        //public new void CopyTo(Stream destination)
        //{
        //    throw new NotSupportedException();
        //}

        //public new Task CopyToAsync(Stream destination, CancellationToken cancellationToken)
        //{
        //    throw new NotSupportedException();
        //}

        //public new Task CopyToAsync(Stream destination)
        //{
        //    throw new NotSupportedException();
        //}

        //public new Task CopyToAsync(Stream destination, int bufferSize)
        //{
        //    throw new NotSupportedException();
        //}

        //public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        //{
        //    throw new NotSupportedException();
        //}

        //public override ValueTask DisposeAsync()
        //{
        //    throw new NotSupportedException();
        //}

        //public override int EndRead(IAsyncResult asyncResult)
        //{
        //    throw new NotSupportedException();
        //}

        //public override void EndWrite(IAsyncResult asyncResult)
        //{
        //    throw new NotSupportedException();
        //}

        //public override Task FlushAsync(CancellationToken cancellationToken)
        //{
        //    throw new NotSupportedException();
        //}

        //public override int Read(Span<byte> buffer)
        //{
        //    throw new NotSupportedException();
        //}

        //public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        //{
        //    throw new NotSupportedException();
        //}

        //public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        //{
        //    throw new NotSupportedException();
        //}

        //public override int ReadByte()
        //{
        //    throw new NotSupportedException();
        //}

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        //public new Task WriteAsync(byte[] buffer, int offset, int count)
        //{
        //    throw new NotSupportedException();
        //}

        //public override void Write(ReadOnlySpan<byte> buffer)
        //{
        //    throw new NotSupportedException();
        //}

        //public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        //{
        //    throw new NotSupportedException();
        //}

        //public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        //{
        //    throw new NotSupportedException();
        //}

        //public override void WriteByte(byte value)
        //{
        //    throw new NotSupportedException();
        //}

#pragma warning restore CS8632
#pragma warning restore IDE0060

        #endregion
    }
}