//! Length-prefixed frames and JSON envelopes.

use lingo_core::msg::{CodecError, Envelope, decode_line, encode_line};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("frame too large: {0} bytes")]
    TooLarge(usize),
    #[error("message codec error: {0}")]
    Codec(#[from] CodecError),
}

pub async fn write_frame<W>(writer: &mut W, payload: &[u8]) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    write_frame_limited(writer, payload, MAX_FRAME_BYTES).await
}

pub async fn write_frame_limited<W>(
    writer: &mut W,
    payload: &[u8],
    limit: usize,
) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    if payload.len() > limit {
        return Err(FrameError::TooLarge(payload.len()));
    }
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_frame<R>(reader: &mut R) -> Result<Vec<u8>, FrameError>
where
    R: AsyncRead + Unpin,
{
    read_frame_limited(reader, MAX_FRAME_BYTES).await
}

pub async fn read_frame_limited<R>(reader: &mut R, limit: usize) -> Result<Vec<u8>, FrameError>
where
    R: AsyncRead + Unpin,
{
    let len = reader.read_u32().await? as usize;
    if len > limit {
        return Err(FrameError::TooLarge(len));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

pub async fn write_envelope<W>(writer: &mut W, envelope: &Envelope) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    write_frame(writer, encode_line(envelope)?.as_bytes()).await
}

pub async fn read_envelope<R>(reader: &mut R) -> Result<Envelope, FrameError>
where
    R: AsyncRead + Unpin,
{
    let payload = read_frame(reader).await?;
    let text = String::from_utf8_lossy(&payload);
    Ok(decode_line(&text)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lingo_core::msg::{Empty, SetAvatar};

    #[tokio::test]
    async fn frame_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(64);
        write_frame(&mut a, b"hello").await.unwrap();
        assert_eq!(read_frame(&mut b).await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn rejects_oversized_frames() {
        let (mut a, _b) = tokio::io::duplex(64);
        let payload = vec![0u8; MAX_FRAME_BYTES + 1];
        assert!(matches!(
            write_frame(&mut a, &payload).await,
            Err(FrameError::TooLarge(_))
        ));
    }

    #[tokio::test]
    async fn honours_a_smaller_limit_for_writes() {
        let (mut a, _b) = tokio::io::duplex(64);
        assert!(matches!(
            write_frame_limited(&mut a, b"hello", 4).await,
            Err(FrameError::TooLarge(5))
        ));
    }

    #[tokio::test]
    async fn honours_a_smaller_limit_for_reads() {
        let (mut a, mut b) = tokio::io::duplex(64);
        tokio::io::AsyncWriteExt::write_all(&mut a, &[0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o'])
            .await
            .unwrap();
        assert!(matches!(
            read_frame_limited(&mut b, 4).await,
            Err(FrameError::TooLarge(5))
        ));
    }

    #[tokio::test]
    async fn envelope_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        let envelope = Envelope::SetAvatar(SetAvatar::default());
        write_envelope(&mut a, &envelope).await.unwrap();
        let decoded = read_envelope(&mut b).await.unwrap();
        assert_eq!(decoded.kind(), "set_avatar");
    }

    #[tokio::test]
    async fn empty_envelope_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(1024);
        write_envelope(&mut a, &Envelope::SetGuestMode(Empty {}))
            .await
            .unwrap();
        assert_eq!(
            read_envelope(&mut b).await.unwrap().kind(),
            "set_guest_mode"
        );
    }
}
