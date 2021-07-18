using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using MDBFS.FileSystem.BinaryStorage.Streams;
using MDBFS.Filesystem.Models;
using MongoDB.Driver;

namespace MDBFS.Filesystem.Streams
{
    public class FileUploadStream : Stream
    {
        private readonly IMongoCollection<Element> _elements;
        private readonly Element _file;
        private BinaryUploadStream _bus;

        public FileUploadStream(BinaryUploadStream bus, IMongoCollection<Element> elements, Element file)
        {
            _bus = bus;
            _elements = elements;
            _file = file;
            _file.Id = bus.Id;
            _file.Opened = DateTime.Now;
        }

        public override bool CanRead => _bus.CanRead;
        public override bool CanSeek => _bus.CanSeek;
        public override bool CanWrite => _bus.CanWrite;
        public override bool CanTimeout => _bus.CanTimeout;
        public override long Length => _bus.Length;

        public string Id => _file.Id;
        public Element Element { get; private set; }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _bus.Write(buffer, offset, count);
            _file.IncreaseLength(count);
            _file.Modified = DateTime.Now;
        }

        public new async Task WriteAsync(byte[] buffer, int offset, int count)
        {
            await _bus.WriteAsync(buffer, offset, count);
            _file.IncreaseLength(count);
            _file.Modified = DateTime.Now;
        }


        public override void Flush()
        {
            if (_bus != null)
            {
                _bus.Flush();
                Element = _file;
                _elements.InsertOne(Element);
                _bus = null;
            }
        }

        public new async Task FlushAsync()
        {
            if (_bus != null)
            {
                await _bus.FlushAsync();
                await _elements.InsertOneAsync(_file);
                _bus = null;
            }
        }

        public new void Dispose()
        {
            Flush();
        }

        public override void Close()
        {
            Flush();
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

        public new static Stream Synchronized(Stream stream)
        {
            throw new NotSupportedException();
        }

        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback,
            object? state)
        {
            throw new NotSupportedException();
        }

        public override IAsyncResult BeginWrite(byte[] buffer, int offset, int count, AsyncCallback callback,
            object? state)
        {
            throw new NotSupportedException();
        }

        public override void CopyTo(Stream destination, int bufferSize)
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

        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public override ValueTask DisposeAsync()
        {
            throw new NotSupportedException();
        }

        public override int EndRead(IAsyncResult asyncResult)
        {
            throw new NotSupportedException();
        }

        public override void EndWrite(IAsyncResult asyncResult)
        {
            throw new NotSupportedException();
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public override int Read(Span<byte> buffer)
        {
            throw new NotSupportedException();
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public new Task<int> ReadAsync(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override int ReadByte()
        {
            throw new NotSupportedException();
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            throw new NotSupportedException();
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public override void WriteByte(byte value)
        {
            throw new NotSupportedException();
        }

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