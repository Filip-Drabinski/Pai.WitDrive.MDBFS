using System;

namespace MDBFS.Misc
{
    public static class ExtensionMethods
    {
        public static T[] Append<T>(this T[] source, T[] other)
        {
            var result = new T[source.Length + other.Length];
            Array.Copy(source, 0, result, 0, source.Length);
            Array.Copy(other, 0, result, source.Length, other.Length);
            return result;
        }

        public static T[] Prepend<T>(this T[] source, T[] other)
        {
            var result = new T[source.Length + other.Length];
            Array.Copy(other, 0, result, 0, other.Length);
            Array.Copy(source, 0, result, other.Length, source.Length);
            return result;
        }

        public static T[] SubArray<T>(this T[] source, int index, int length)
        {
            var result = new T[length - index];
            Array.Copy(source, index, result, 0, length);
            return result;
        }

        public static T[][] Split<T>(this T[] source, int maxLength)
        {
            var numParts = (int) MathF.Ceiling(source.Length / (float) maxLength);
            var result = new T[numParts][];
            for (var itP = 0; itP < numParts - 1; itP++)
            {
                var part = new T[maxLength];
                Array.Copy(source, itP * maxLength, part, 0, maxLength);
                result[itP] = part;
            }

            result[^1] = new T[source.Length - (numParts - 1) * maxLength];
            Array.Copy(source, (numParts - 1) * maxLength, result[^1], 0, result[^1].Length);
            return result;
        }
    }
}