using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using MDBFS.FileSystem.BinaryStorage.Models;
using MDBFS.Misc;
using MongoDB.Driver;

namespace MDBFS.FileSystem.BinaryStorage.Streams
{
    public class BinaryUploadStream : Stream
    {
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override bool CanTimeout => false;
        public override long Length => _map.Length;
        public string Id => _map.Id;

        private readonly NamedReaderWriterLock _nrwl;
        private string _nrwlId; //todo: add to constructor
        private readonly IMongoCollection<Chunk> _chunks;
        private readonly IMongoCollection<ChunkMap> _maps;
        private ChunkMap _map;
        private byte[] _writeBuffer;
        private readonly int _maxChunkLength;

        public static (bool success, BinaryUploadStream stream) Open(IMongoCollection<ChunkMap> maps,
            IMongoCollection<Chunk> chunks, int maxChunkLenght, string id, NamedReaderWriterLock namedReaderWriterLock)
        {
            var (success, map) = CreateNewElement(maps, id, namedReaderWriterLock);
            if (!success) return (false, null);


            var stream = new BinaryUploadStream(maps, chunks, maxChunkLenght, namedReaderWriterLock)
            {
                _map = map
            };
            stream._nrwlId = namedReaderWriterLock.AcquireWriterLock($"{nameof(Chunk)}.{map.Id}");

            return (true, stream);
        }

        public static async Task<(bool success, BinaryUploadStream stream)> OpenAsync(IMongoCollection<ChunkMap> maps,
            IMongoCollection<Chunk> chunks, int maxChunkLenght, string id, NamedReaderWriterLock namedReaderWriterLock)
        {
            if (id != null)
            {
            }

            var (success, map) = await CreateNewElementAsync(maps, id, namedReaderWriterLock);
            if (!success) return (false, null);

            var stream = new BinaryUploadStream(maps, chunks, maxChunkLenght, namedReaderWriterLock)
            {
                _map = map
            };
            stream._nrwlId = await namedReaderWriterLock.AcquireWriterLockAsync($"{nameof(Chunk)}.{map.Id}");
            return (true, stream);
        }

        private static (bool success, ChunkMap map) CreateNewElement(IMongoCollection<ChunkMap> maps, string id,
            NamedReaderWriterLock nrwl)
        {
            var map = new ChunkMap {Id = id, ChunksIDs = new List<string>(), Length = 0};
            var lId = nrwl.AcquireReaderLock($"{nameof(ChunkMap)}.{id}");
            using var session = maps.Database.Client.StartSession();
            session.StartTransaction();
            try
            {
                if (id != null)
                {
                    if (!maps.Find(x => x.Id == id).Any())
                    {
                        map.Id = id;
                    }
                    else
                    {
                        session.AbortTransaction();
                        nrwl.ReleaseLock($"{nameof(ChunkMap)}.{id}", lId);
                        return (false, null);
                    }
                }

                maps.InsertOne(map);

                session.CommitTransaction();
                nrwl.ReleaseLock($"{nameof(ChunkMap)}.{id}", lId);
                return (true, map);
            }
            catch
            {
                nrwl.ReleaseLock($"{nameof(ChunkMap)}.{id}", lId);
            }

            session.AbortTransaction();
            nrwl.ReleaseLock($"{nameof(ChunkMap)}.{id}", lId);
            return (false, null);
        }

        private static async Task<(bool success, ChunkMap map)> CreateNewElementAsync(IMongoCollection<ChunkMap> maps,
            string id, NamedReaderWriterLock nrwl)
        {
            var map = new ChunkMap {Id = id, ChunksIDs = new List<string>(), Length = 0};
            var lId = await nrwl.AcquireReaderLockAsync($"{nameof(ChunkMap)}.{id}");
            using var session = await maps.Database.Client.StartSessionAsync();
            session.StartTransaction();
            try
            {
                if (id != null)
                {
                    if (!await (await maps.FindAsync(x => x.Id == id)).AnyAsync())
                    {
                        map.Id = id;
                    }
                    else
                    {
                        await nrwl.ReleaseLockAsync($"{nameof(ChunkMap)}.{id}", lId);
                        return (false, null);
                    }
                }

                await maps.InsertOneAsync(map);

                await session.CommitTransactionAsync();
                await nrwl.ReleaseLockAsync($"{nameof(ChunkMap)}.{id}", lId);
                return (true, map);
            }
            catch (Exception)
            {
                await nrwl.ReleaseLockAsync($"{nameof(ChunkMap)}.{id}", lId);
            }

            await session.AbortTransactionAsync();
            await nrwl.ReleaseLockAsync($"{nameof(ChunkMap)}.{id}", lId);
            return (false, null);
        }


        private BinaryUploadStream(IMongoCollection<ChunkMap> maps, IMongoCollection<Chunk> chunks, int maxChunkLength,
            NamedReaderWriterLock namedReaderWriterLock)
        {
            _maps = maps;
            _chunks = chunks;
            _maxChunkLength = maxChunkLength;
            _nrwl = namedReaderWriterLock;
            _writeBuffer = new byte[0];
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            buffer = buffer.SubArray(offset, count);
            _writeBuffer = _writeBuffer.Append(buffer);
            if (_writeBuffer.Length < _maxChunkLength)
            {
            }
            else if (_writeBuffer.Length == _maxChunkLength)
            {
                SaveBytesInDb(_writeBuffer);
                _writeBuffer = new byte[0];
            }
            else if (_writeBuffer.Length > _maxChunkLength)
            {
                var parts = _writeBuffer.Split(_maxChunkLength);
                for (var itP = 0; itP < parts.Length - 1; itP++) SaveBytesInDb(parts[itP]);
                _writeBuffer = parts[^1];
            }
        }

        public new async Task WriteAsync(byte[] buffer, int offset, int count)
        {
            buffer = buffer.SubArray(offset, count);
            _writeBuffer = _writeBuffer.Append(buffer);
            if (_writeBuffer.Length < _maxChunkLength)
            {
            }
            else if (_writeBuffer.Length == _maxChunkLength)
            {
                await SaveBytesInDbAsync(_writeBuffer);
                _writeBuffer = new byte[0];
            }
            else if (_writeBuffer.Length > _maxChunkLength)
            {
                var parts = _writeBuffer.Split(_maxChunkLength);
                for (var itP = 0; itP < parts.Length - 1; itP++) await SaveBytesInDbAsync(parts[itP]);
                _writeBuffer = parts[^1];
            }
        }

        private void SaveBytesInDb(byte[] data)
        {
            if (data.Length == 0) return;
            using var session = _maps.Database.Client.StartSession();
            session.StartTransaction();
            try
            {
                var b = new Chunk {Bytes = data};
                _chunks.InsertOne(b);
                _map.Length += data.Length;
                _map.ChunksIDs.Add(b.Id);
                _maps.FindOneAndReplace(x => x.Id == _map.Id, _map);

                session.CommitTransaction();
            }
            catch (Exception)
            {
                session.AbortTransaction();
                throw;
            }
        }

        private async Task SaveBytesInDbAsync(byte[] data)
        {
            if (data.Length == 0) return;
            using var session = await _maps.Database.Client.StartSessionAsync();
            session.StartTransaction();
            try
            {
                var b = new Chunk {Bytes = data};
                await _chunks.InsertOneAsync(b);
                _map.Length += data.Length;
                _map.ChunksIDs.Add(b.Id);
                await _maps.FindOneAndReplaceAsync(x => x.Id == _map.Id, _map);

                await session.CommitTransactionAsync();
            }
            catch (Exception)
            {
                await session.AbortTransactionAsync();
                throw;
            }
        }

        public List<ChunkMap> GetRemoved()
        {
            return _maps.Find(x => x.Removed).ToList();
        }

        public override void Flush()
        {
            if (_writeBuffer != null)
            {
                SaveBytesInDb(_writeBuffer);
                _nrwl.ReleaseLock($"{nameof(Chunk)}.{_map.Id}", _nrwlId);
                _writeBuffer = null;
                var lId = _nrwl.AcquireWriterLock($"{nameof(ChunkMap)}.{_map.Id}");
                _maps.UpdateOne(x => x.Id == _map.Id, Builders<ChunkMap>.Update.Set(x => x.Removed, false));
                _nrwl.ReleaseLock($"{nameof(ChunkMap)}.{_map.Id}", lId);
            }
        }

        public new async Task FlushAsync()
        {
            if (_writeBuffer != null)
            {
                await SaveBytesInDbAsync(_writeBuffer);
                await _nrwl.ReleaseLockAsync($"{nameof(Chunk)}.{_map.Id}", _nrwlId);
                _writeBuffer = null;
                var lId = await _nrwl.AcquireWriterLockAsync($"{nameof(ChunkMap)}.{_map.Id}");
                await _maps.UpdateOneAsync(x => x.Id == _map.Id, Builders<ChunkMap>.Update.Set(x => x.Removed, false));
                await _nrwl.ReleaseLockAsync($"{nameof(ChunkMap)}.{_map.Id}", lId);
            }
        }

        public new void Dispose()
        {
            if (_writeBuffer != null) Flush();
        }

        public override void Close()
        {
            if (_writeBuffer != null) Flush();
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

        public override long Position
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

        //public new Task<int> ReadAsync(byte[] buffer, int offset, int count)
        //{
        //    throw new NotSupportedException();
        //}

        //public override int ReadByte()
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

        //[Obsolete]
        //protected override WaitHandle CreateWaitHandle()
        //{
        //    throw new NotSupportedException();
        //}

        //protected override void Dispose(bool disposing)
        //{
        //    throw new NotSupportedException();
        //}

        //[Obsolete]
        //protected override void ObjectInvariant()
        //{
        //    throw new NotSupportedException();
        //}

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }
#pragma warning restore CS8632
#pragma warning restore IDE0060

        #endregion
    }
}