using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MDBFS.Exceptions;
using MDBFS.FileSystem.BinaryStorage.Streams;
using MDBFS.Filesystem.Models;
using MongoDB.Driver;

namespace MDBFS.Filesystem.Streams
{
    public class FileDownloadStream : Stream
    {
        private readonly Element _file;
        private BinaryDownloadStream _bds;

        public FileDownloadStream(BinaryDownloadStream bds, IMongoCollection<Element> elements, string id)
        {
            var fSearch = elements.Find(x => x.Id == id && x.Removed == false).ToList();
            if (fSearch.Count == 0) throw new MdbfsElementNotFoundException();
            _file = fSearch.First();
            _bds = bds;
        }

        public override long Position
        {
            get => _bds.Position;
            set => _bds.Position = value;
        }

        public override long Length => _bds.Length;
        public override bool CanWrite => _bds.CanWrite;
        public new bool CanTimeout => _bds.CanTimeout;
        public override bool CanSeek => _bds.CanSeek;
        public override bool CanRead => _bds.CanRead;

        public Element Element { get; private set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            return _bds.Read(buffer, offset, count);
        }

        public new async Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            return await _bds.ReadAsync(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _bds.Seek(offset, origin);
        }

        public async Task<long> SeekAsync(long offset, SeekOrigin origin)
        {
            return await _bds.SeekAsync(offset, origin);
        }

        public override void Flush()
        {
            if (_bds != null)
            {
                Element = _file;
                _bds.Flush();
                _bds = null;
            }
        }

        public new void Dispose()
        {
            Flush();
        }

        public new void Close()
        {
            Flush();
        }

        #region NotSupported

#pragma warning disable IDE0060
#pragma warning disable CS8632
        public new int ReadTimeout
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public new int WriteTimeout
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public new Task FlushAsync()
        {
            throw new NotSupportedException();
        }

        public new IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object? state)
        {
            throw new NotSupportedException();
        }

        public new IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback, object? state)
        {
            throw new NotSupportedException();
        }

        public new void CopyTo(Stream destination, int bufferSize)
        {
            throw new NotSupportedException();
        }

        public new void CopyTo(Stream destination)
        {
            throw new NotSupportedException();
        }

        public new Task CopyToAsync(Stream destination, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public new Task CopyToAsync(Stream destination)
        {
            throw new NotSupportedException();
        }

        public new Task CopyToAsync(Stream destination, int bufferSize)
        {
            throw new NotSupportedException();
        }

        public new Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public new ValueTask DisposeAsync()
        {
            throw new NotSupportedException();
        }

        public new int EndRead(IAsyncResult asyncResult)
        {
            throw new NotSupportedException();
        }

        public new void EndWrite(IAsyncResult asyncResult)
        {
            throw new NotSupportedException();
        }

        public new Task FlushAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public new int Read(Span<byte> buffer)
        {
            throw new NotSupportedException();
        }

        public new ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public new Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public new int ReadByte()
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public new void Write(ReadOnlySpan<byte> buffer)
        {
            throw new NotSupportedException();
        }

        public new Task WriteAsync(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public new Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public new ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public new void WriteByte(byte value)
        {
            throw new NotSupportedException();
        }
#pragma warning restore CS8632
#pragma warning restore IDE0060

        #endregion
    }
}