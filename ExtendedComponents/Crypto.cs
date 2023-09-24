using System.Security.Cryptography;
using System.Text;

namespace ExtendedComponents;

public static class Crypto
{
    public static byte[] GenerateSecureBytes(uint length)
    {
        var bytes = new byte[length];
        var rand = RandomNumberGenerator.Create();
        rand.GetBytes(bytes);
        return bytes;
    }

    public static string GenerateSecureString(uint length)
    {
        var bytes = GenerateSecureBytes(length);
        StringBuilder sb = new();
        foreach (var c in bytes)
        {
            sb.Append(Convert.ToChar(Convert.ToInt32(Math.Floor(26 * (Convert.ToSingle(c) / 255.0f) + 65))));
        }
        return sb.ToString();
    }
    public static byte[] HashSha256(string inputString)
    {
        return SHA256.HashData(Encoding.UTF8.GetBytes(inputString));
    }
    public static string HashSha256String(string inputString)
    {
        StringBuilder sb = new();
        foreach (var b in HashSha256(inputString))
            sb.Append(b.ToString("X2"));

        return sb.ToString();
    }
}