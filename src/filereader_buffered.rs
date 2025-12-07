use std::collections::VecDeque;
use std::error::Error;

use crate::filedevice::{ReadResult, Reader};

/// A buffered reader that wraps another Reader and provides an intermediate
/// ring buffer to absorb timing jitter from bursty input sources like Bluetooth.
///
/// This is particularly useful for BlueZ/Bluetooth audio where data arrives
/// in variable-timed bursts rather than a steady stream.
pub struct BufferedReader<R> {
    inner: R,
    buffer: VecDeque<u8>,
    buffer_capacity: usize,
}

impl<R: Reader> BufferedReader<R> {
    /// Create a new BufferedReader with the specified buffer capacity.
    ///
    /// # Arguments
    /// * `inner` - The underlying Reader to read from
    /// * `buffer_capacity` - Maximum number of bytes to buffer. This should be
    ///   large enough to absorb timing variations in the input stream.
    ///   A typical value for Bluetooth would be 3-4x the chunk size in bytes.
    pub fn new(inner: R, buffer_capacity: usize) -> Self {
        BufferedReader {
            inner,
            buffer: VecDeque::with_capacity(buffer_capacity),
            buffer_capacity,
        }
    }

    /// Try to fill the internal buffer by reading from the inner reader.
    /// Returns the ReadResult from the inner reader, or Complete if buffer is full.
    fn fill_buffer(&mut self, requested_bytes: usize) -> Result<ReadResult, Box<dyn Error>> {
        // Calculate how much space we have in the buffer
        let space_available = self.buffer_capacity.saturating_sub(self.buffer.len());

        if space_available == 0 {
            // Buffer is full, no need to read
            return Ok(ReadResult::Complete(0));
        }

        // Read into a temporary buffer
        // We want to read enough to satisfy the request plus some extra for buffering
        let bytes_to_read = space_available.min(requested_bytes.max(self.buffer_capacity / 2));
        let mut temp_buf = vec![0u8; bytes_to_read];

        let result = self.inner.read(&mut temp_buf)?;

        let bytes_read = match &result {
            ReadResult::Complete(n) | ReadResult::Timeout(n) | ReadResult::EndOfFile(n) => *n,
        };

        // Append read bytes to our buffer
        for byte in temp_buf.iter().take(bytes_read) {
            self.buffer.push_back(*byte);
        }

        Ok(result)
    }

    /// Drain bytes from the internal buffer into the output slice.
    /// Returns the number of bytes drained.
    fn drain_buffer(&mut self, data: &mut [u8]) -> usize {
        let bytes_to_drain = data.len().min(self.buffer.len());

        for (i, byte) in self.buffer.drain(..bytes_to_drain).enumerate() {
            data[i] = byte;
        }

        bytes_to_drain
    }

    /// Returns the current number of bytes in the buffer.
    #[allow(dead_code)]
    pub fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Returns the buffer capacity.
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.buffer_capacity
    }
}

impl<R: Reader> Reader for BufferedReader<R> {
    fn read(&mut self, data: &mut [u8]) -> Result<ReadResult, Box<dyn Error>> {
        let requested = data.len();
        let mut total_bytes_read = 0;
        let mut saw_eof = false;
        let mut saw_timeout = false;

        // First, drain any bytes we already have buffered
        if !self.buffer.is_empty() {
            total_bytes_read = self.drain_buffer(data);
            if total_bytes_read >= requested {
                return Ok(ReadResult::Complete(total_bytes_read));
            }
        }

        // Keep reading until we have enough data or hit EOF/timeout
        loop {
            let remaining = requested - total_bytes_read;
            let fill_result = self.fill_buffer(remaining)?;

            match fill_result {
                ReadResult::EndOfFile(_) => {
                    saw_eof = true;
                }
                ReadResult::Timeout(_) => {
                    saw_timeout = true;
                }
                ReadResult::Complete(_) => {}
            }

            // Drain newly buffered bytes
            let newly_drained = self.drain_buffer(&mut data[total_bytes_read..]);
            total_bytes_read += newly_drained;

            if total_bytes_read >= requested {
                return Ok(ReadResult::Complete(total_bytes_read));
            }

            // If we hit EOF or timeout and couldn't get more data, return what we have
            if saw_eof {
                return Ok(ReadResult::EndOfFile(total_bytes_read));
            }
            if saw_timeout {
                return Ok(ReadResult::Timeout(total_bytes_read));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Cursor};

    /// A mock Reader that delivers data in configurable bursts
    struct BurstyReader {
        data: Vec<u8>,
        position: usize,
        burst_sizes: Vec<usize>,
        burst_index: usize,
        timeout_after_bursts: bool,
    }

    impl BurstyReader {
        fn new(data: Vec<u8>, burst_sizes: Vec<usize>) -> Self {
            BurstyReader {
                data,
                position: 0,
                burst_sizes,
                burst_index: 0,
                timeout_after_bursts: false,
            }
        }

        fn with_timeout_after_bursts(mut self) -> Self {
            self.timeout_after_bursts = true;
            self
        }
    }

    impl Reader for BurstyReader {
        fn read(&mut self, data: &mut [u8]) -> Result<ReadResult, Box<dyn Error>> {
            // Determine how many bytes to deliver in this burst
            let burst_size = if self.burst_index < self.burst_sizes.len() {
                self.burst_sizes[self.burst_index]
            } else if self.timeout_after_bursts {
                // Simulate timeout - return 0 bytes (no more bursts configured)
                return Ok(ReadResult::Timeout(0));
            } else {
                // Default: deliver requested amount
                data.len()
            };

            self.burst_index += 1;

            if self.position >= self.data.len() {
                return Ok(ReadResult::EndOfFile(0));
            }

            let available = self.data.len() - self.position;
            let to_read = burst_size.min(available).min(data.len());

            data[..to_read].copy_from_slice(&self.data[self.position..self.position + to_read]);
            self.position += to_read;

            if self.position >= self.data.len() {
                Ok(ReadResult::EndOfFile(to_read))
            } else {
                Ok(ReadResult::Complete(to_read))
            }
        }
    }

    /// A simple blocking reader wrapper for testing
    struct SimpleReader<R> {
        inner: R,
    }

    impl<R: io::Read> SimpleReader<R> {
        fn new(inner: R) -> Self {
            SimpleReader { inner }
        }
    }

    impl<R: io::Read> Reader for SimpleReader<R> {
        fn read(&mut self, data: &mut [u8]) -> Result<ReadResult, Box<dyn Error>> {
            match self.inner.read(data) {
                Ok(0) => Ok(ReadResult::EndOfFile(0)),
                Ok(n) => Ok(ReadResult::Complete(n)),
                Err(e) => Err(Box::new(e)),
            }
        }
    }

    #[test]
    fn test_buffered_reader_basic() {
        // Create a simple reader with some data
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let inner = SimpleReader::new(Cursor::new(data.clone()));
        let mut reader = BufferedReader::new(inner, 32);

        // Read all data
        let mut buf = vec![0u8; 10];
        let result = reader.read(&mut buf).unwrap();

        assert!(matches!(result, ReadResult::Complete(10) | ReadResult::EndOfFile(10)));
        assert_eq!(buf, data);
    }

    #[test]
    fn test_buffered_reader_bursty_input() {
        // Simulate Bluetooth-like bursty input:
        // Data arrives in bursts of 8 bytes, but we request 4 bytes at a time
        let data: Vec<u8> = (0..32).collect();
        let inner = BurstyReader::new(data.clone(), vec![8, 8, 8, 8]);
        let mut reader = BufferedReader::new(inner, 32);

        // Request 4 bytes - should get them from the first burst of 8
        let mut buf = vec![0u8; 4];
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));
        assert_eq!(buf, vec![0, 1, 2, 3]);

        // Request 4 more - should come from the buffered remainder
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));
        assert_eq!(buf, vec![4, 5, 6, 7]);

        // Request 4 more - triggers next burst
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));
        assert_eq!(buf, vec![8, 9, 10, 11]);
    }

    #[test]
    fn test_buffered_reader_absorbs_timing_gap() {
        // Simulate: burst of 16 bytes, then timeout, then we should still
        // be able to read from the buffer
        let data: Vec<u8> = (0..16).collect();
        let inner = BurstyReader::new(data, vec![16]).with_timeout_after_bursts();
        let mut reader = BufferedReader::new(inner, 32);

        // First read: gets the burst and buffers it
        let mut buf = vec![0u8; 4];
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));
        assert_eq!(buf, vec![0, 1, 2, 3]);
        assert_eq!(reader.buffered_bytes(), 12); // 16 - 4 = 12 remaining

        // Second read: should succeed from buffer despite timeout from inner
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));
        assert_eq!(buf, vec![4, 5, 6, 7]);
        assert_eq!(reader.buffered_bytes(), 8);

        // Third read: still from buffer
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));
        assert_eq!(buf, vec![8, 9, 10, 11]);
        assert_eq!(reader.buffered_bytes(), 4);

        // Fourth read: last of the buffered data
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));
        assert_eq!(buf, vec![12, 13, 14, 15]);
        assert_eq!(reader.buffered_bytes(), 0);

        // Fifth read: buffer empty, inner times out
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Timeout(0)));
    }

    #[test]
    fn test_buffered_reader_eof() {
        let data = vec![1, 2, 3, 4, 5];
        let inner = SimpleReader::new(Cursor::new(data));
        let mut reader = BufferedReader::new(inner, 32);

        // Request more than available
        let mut buf = vec![0u8; 10];
        let result = reader.read(&mut buf).unwrap();

        assert!(matches!(result, ReadResult::EndOfFile(5)));
        assert_eq!(&buf[..5], &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_buffered_reader_partial_then_complete() {
        // First burst is smaller than requested, second completes it
        let data: Vec<u8> = (0..16).collect();
        let inner = BurstyReader::new(data, vec![2, 6, 8]);
        let mut reader = BufferedReader::new(inner, 32);

        // Request 8 bytes - first burst gives 2, second gives 6
        let mut buf = vec![0u8; 8];
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(8)));
        assert_eq!(buf, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_buffered_reader_buffer_capacity_respected() {
        // Ensure we don't exceed buffer capacity
        let data: Vec<u8> = (0..100).collect();
        let inner = BurstyReader::new(data, vec![100]); // One big burst
        let mut reader = BufferedReader::new(inner, 16); // Small buffer

        // Read small amount, buffer should fill but not exceed capacity
        let mut buf = vec![0u8; 4];
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::Complete(4)));

        // Buffer should have at most capacity - what we read
        assert!(reader.buffered_bytes() <= 16);
    }

    #[test]
    fn test_buffered_reader_empty_input() {
        let data: Vec<u8> = vec![];
        let inner = SimpleReader::new(Cursor::new(data));
        let mut reader = BufferedReader::new(inner, 32);

        let mut buf = vec![0u8; 10];
        let result = reader.read(&mut buf).unwrap();
        assert!(matches!(result, ReadResult::EndOfFile(0)));
    }

    #[test]
    fn test_buffered_reader_large_request() {
        // Request more than buffer capacity
        let data: Vec<u8> = (0..64).collect();
        let inner = BurstyReader::new(data.clone(), vec![64]);
        let mut reader = BufferedReader::new(inner, 16);

        let mut buf = vec![0u8; 32];
        let result = reader.read(&mut buf).unwrap();

        // Should still work, just might take multiple internal reads
        match result {
            ReadResult::Complete(n) | ReadResult::EndOfFile(n) => {
                assert!(n > 0);
                assert_eq!(&buf[..n], &data[..n]);
            }
            ReadResult::Timeout(_) => panic!("Unexpected timeout"),
        }
    }
}
