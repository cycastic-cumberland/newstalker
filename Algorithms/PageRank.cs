using NumSharp;

namespace Algorithms;

public class PageRank
{
    public static float[] Rank(float[,] matrix, int iterationCount = 100, float damping = 0.5f)
    {
        var m = np.array(matrix);
        var n = m.shape[1];
        var v = np.ones(n) / n;
        var mHat = (damping * m + (1 - damping) / n);
        for (int i = 0; i < iterationCount; i++)
        {
            v = np.dot(mHat, v);
        }

        float[] ret = v.Data<float>().ToArray();
        return ret;
    } 
}