package com.qinjiangbo;

/**
 * @date: 05/05/2017 1:45 PM
 * @author: qinjiangbo@github.io
 */
public class GibbsSamplerApp {

    /**
     * document data (term lists)<br>
     * 文档
     */
    int[][] documents;

    /**
     * vocabulary size<br>
     * 词表大小
     */
    int V;

    /**
     * number of topics<br>
     * 主题数目
     */
    int K;

    /**
     * Dirichlet parameter (document--topic associations)<br>
     * 文档——主题参数
     */
    double alpha = 2.0;

    /**
     * Dirichlet parameter (topic--term associations)<br>
     * 主题——词语参数
     */
    double beta = 0.5;

    /**
     * topic assignments for each word.<br>
     * 每个词语的主题 z[i][j] := 文档i的第j个词语的主题编号
     */
    int z[][];

    /**
     * cwt[i][j] number of instances of word i (term?) assigned to topic j.<br>
     * 计数器，nw[i][j] := 词语i归入主题j的次数
     */
    int[][] nw;

    /**
     * na[i][j] number of words in document i assigned to topic j.<br>
     * 计数器，nd[i][j] := 文档[i]中归入主题j的词语的个数
     */
    int[][] nd;

    /**
     * nwsum[j] total number of words assigned to topic j.<br>
     * 计数器，nwsum[j] := 归入主题j词语的个数
     */
    int[] nwsum;

    /**
     * nasum[i] total number of words in document i.<br>
     * 计数器,ndsum[i] := 文档i中全部词语的数量
     */
    int[] ndsum;

    /**
     * cumulative statistics of theta<br>
     * theta的累积量
     */
    double[][] thetasum;

    /**
     * cumulative statistics of phi<br>
     * phi的累积量
     */
    double[][] phisum;

    /**
     * size of statistics<br>
     * 样本容量
     */
    int numstats;

    /**
     * burn-in period<br>
     * 收敛前的迭代次数
     */
    private static int BURN_IN = 100;

    /**
     * max iterations<br>
     * 最大迭代次数
     */
    private static int ITERATIONS = 1000;

    /**
     * sample lag (if -1 only one sample taken)<br>
     * 最后的模型个数（取收敛后的n个迭代的参数做平均可以使得模型质量更高）
     */
    private static int SAMPLE_LAG = 10;

    /**
     * Initialise the Gibbs sampler with data.<br>
     * 用数据初始化采样器
     *
     * @param documents 文档
     * @param V         vocabulary size 词表大小
     */
    public GibbsSamplerApp(int[][] documents, int V) {

        this.documents = documents;
        this.V = V;
    }

    /**
     * Initialisation: Must start with an assignment of observations to topics ?
     * Many alternatives are possible, I chose to perform random assignments
     * with equal probabilities<br>
     * 随机初始化状态
     *
     * @param K number of topics K个主题
     */
    public void initialState(int K) {
        int M = documents.length;

        // initialise count variables. 初始化计数器
        nw = new int[V][K];
        nd = new int[M][K];
        nwsum = new int[K];
        ndsum = new int[M];

        // The z_i are are initialised to values in [1,K] to determine the
        // initial state of the Markov chain.

        z = new int[M][];   // z_i := 1到K之间的值，表示马氏链的初始状态
        for (int m = 0; m < M; m++) {
            int N = documents[m].length;
            z[m] = new int[N];
            for (int n = 0; n < N; n++) {
                int topic = (int) (Math.random() * K);
                z[m][n] = topic;
                // number of instances of word i assigned to topic j
                nw[documents[m][n]][topic]++;
                // number of words in document i assigned to topic j.
                nd[m][topic]++;
                // total number of words assigned to topic j.
                nwsum[topic]++;
            }
            // total number of words in document i
            ndsum[m] = N;
        }
    }

    /**
     * Main method: Select initial state ? Repeat a large number of times: 1.
     * Select an element 2. Update conditional on other elements. If
     * appropriate, output summary for each run.<br>
     * 采样
     *
     * @param K     number of topics 主题数
     * @param alpha symmetric prior parameter on document--topic associations 对称文档——主题先验概率？
     * @param beta  symmetric prior parameter on topic--term associations 对称主题——词语先验概率？
     */
    public void gibbs(int K, double alpha, double beta) {
        this.K = K;
        this.alpha = alpha;
        this.beta = beta;

        // init sampler statistics  分配内存
        if (SAMPLE_LAG > 0) {
            thetasum = new double[documents.length][K];
            phisum = new double[K][V];
            numstats = 0;
        }

        // initial state of the Markov chain:
        initialState(K);

        for (int i = 0; i < ITERATIONS; i++) {

            // for all z_i
            for (int m = 0; m < z.length; m++) {
                for (int n = 0; n < z[m].length; n++) {

                    // (z_i = z[m][n])
                    // sample from p(z_i|z_-i, w)
                    int topic = sampleFullConditional(m, n);
                    z[m][n] = topic;
                }
            }

            // get statistics after burn-in
            if ((i > BURN_IN) && (SAMPLE_LAG > 0) && (i % SAMPLE_LAG == 0)) {
                updateParams();
            }
        }
        System.out.println();
    }

    /**
     * Sample a topic z_i from the full conditional distribution: p(z_i = j |
     * z_-i, w) = (n_-i,j(w_i) + beta)/(n_-i,j(.) + W * beta) * (n_-i,j(d_i) +
     * alpha)/(n_-i,.(d_i) + K * alpha) <br>
     * 根据上述公式计算文档m中第n个词语的主题的完全条件分布，输出最可能的主题
     *
     * @param m document
     * @param n word
     */
    private int sampleFullConditional(int m, int n) {

        // remove z_i from the count variables  先将这个词从计数器中抹掉
        int topic = z[m][n];
        nw[documents[m][n]][topic]--;
        nd[m][topic]--;
        nwsum[topic]--;
        ndsum[m]--;

        // do multinomial sampling via cumulative method: 通过多项式方法采样多项式分布
        double[] p = new double[K];
        for (int k = 0; k < K; k++) {
            p[k] = (nw[documents[m][n]][k] + beta) / (nwsum[k] + V * beta)
                    * (nd[m][k] + alpha) / (ndsum[m] + K * alpha);
        }
        // cumulate multinomial parameters  累加多项式分布的参数
        for (int k = 1; k < p.length; k++) {
            p[k] += p[k - 1];
        }
        // scaled sample because of unnormalised p[] 正则化
        double u = Math.random() * p[K - 1];
        for (topic = 0; topic < p.length; topic++) {
            if (u < p[topic])
                break;
        }

        // add newly estimated z_i to count variables   将重新估计的该词语加入计数器
        nw[documents[m][n]][topic]++;
        nd[m][topic]++;
        nwsum[topic]++;
        ndsum[m]++;

        return topic;
    }

    /**
     * Add to the statistics the values of theta and phi for the current state.<br>
     * 更新参数
     */
    private void updateParams() {
        for (int m = 0; m < documents.length; m++) {
            for (int k = 0; k < K; k++) {
                thetasum[m][k] += (nd[m][k] + alpha) / (ndsum[m] + K * alpha);
            }
        }
        for (int k = 0; k < K; k++) {
            for (int w = 0; w < V; w++) {
                phisum[k][w] += (nw[w][k] + beta) / (nwsum[k] + V * beta);
            }
        }
        numstats++;
    }

    /**
     * Retrieve estimated document--topic associations. If sample lag > 0 then
     * the mean value of all sampled statistics for theta[][] is taken.<br>
     * 获取文档——主题矩阵
     *
     * @return theta multinomial mixture of document topics (M x K)
     */
    public double[][] getTheta() {
        double[][] theta = new double[documents.length][K];

        if (SAMPLE_LAG > 0) {
            for (int m = 0; m < documents.length; m++) {
                for (int k = 0; k < K; k++) {
                    theta[m][k] = thetasum[m][k] / numstats;
                }
            }

        } else {
            for (int m = 0; m < documents.length; m++) {
                for (int k = 0; k < K; k++) {
                    theta[m][k] = (nd[m][k] + alpha) / (ndsum[m] + K * alpha);
                }
            }
        }

        return theta;
    }

    /**
     * Retrieve estimated topic--word associations. If sample lag > 0 then the
     * mean value of all sampled statistics for phi[][] is taken.<br>
     * 获取主题——词语矩阵
     *
     * @return phi multinomial mixture of topic words (K x V)
     */
    public double[][] getPhi() {
        double[][] phi = new double[K][V];
        if (SAMPLE_LAG > 0) {
            for (int k = 0; k < K; k++) {
                for (int w = 0; w < V; w++) {
                    phi[k][w] = phisum[k][w] / numstats;
                }
            }
        } else {
            for (int k = 0; k < K; k++) {
                for (int w = 0; w < V; w++) {
                    phi[k][w] = (nw[w][k] + beta) / (nwsum[k] + V * beta);
                }
            }
        }
        return phi;
    }

    /**
     * Configure the gibbs sampler<br>
     * 配置采样器
     *
     * @param iterations number of total iterations
     * @param burnIn     number of burn-in iterations
     * @param sampleLag  sample interval (-1 for just one sample at the end)
     */
    public void configure(int iterations, int burnIn, int sampleLag) {
        ITERATIONS = iterations;
        BURN_IN = burnIn;
        SAMPLE_LAG = sampleLag;
    }

    /**
     * Inference a new document by a pre-trained phi matrix
     *
     * @param phi pre-trained phi matrix
     * @param doc document
     * @return a p array
     */
    public static double[] inference(double alpha, double beta, double[][] phi, int[] doc) {
        int K = phi.length;
        int V = phi[0].length;
        // init

        // initialise count variables. 初始化计数器
        int[][] nw = new int[V][K];
        int[] nd = new int[K];
        int[] nwsum = new int[K];
        int ndsum = 0;

        // The z_i are are initialised to values in [1,K] to determine the
        // initial state of the Markov chain.

        int N = doc.length;
        int[] z = new int[N];   // z_i := 1到K之间的值，表示马氏链的初始状态
        for (int n = 0; n < N; n++) {
            int topic = (int) (Math.random() * K);
            z[n] = topic;
            // number of instances of word i assigned to topic j
            nw[doc[n]][topic]++;
            // number of words in document assigned to topic.
            nd[topic]++;
            // total number of words assigned to topic j.
            nwsum[topic]++;
        }
        // total number of words in document i
        ndsum = N;
        for (int i = 0; i < ITERATIONS; i++) {
            for (int n = 0; n < z.length; n++) {

                // (z_i = z[m][n])
                // sample from p(z_i|z_-i, w)
                // remove z_i from the count variables  先将这个词从计数器中抹掉
                int topic = z[n];
                nw[doc[n]][topic]--;
                nd[topic]--;
                nwsum[topic]--;
                ndsum--;

                // do multinomial sampling via cumulative method: 通过多项式方法采样多项式分布
                double[] p = new double[K];
                for (int k = 0; k < K; k++) {
                    p[k] = phi[k][doc[n]]
                            * (nd[k] + alpha) / (ndsum + K * alpha);
                }
                // cumulate multinomial parameters  累加多项式分布的参数
                for (int k = 1; k < p.length; k++) {
                    p[k] += p[k - 1];
                }
                // scaled sample because of unnormalised p[] 正则化
                double u = Math.random() * p[K - 1];
                for (topic = 0; topic < p.length; topic++) {
                    if (u < p[topic])
                        break;
                }
                if (topic == K) {
                    throw new RuntimeException("the param K or topic is set too small");
                }
                // add newly estimated z_i to count variables   将重新估计的该词语加入计数器
                nw[doc[n]][topic]++;
                nd[topic]++;
                nwsum[topic]++;
                ndsum++;
                z[n] = topic;
            }
        }

        double[] theta = new double[K];

        for (int k = 0; k < K; k++) {
            theta[k] = (nd[k] + alpha) / (ndsum + K * alpha);
        }
        return theta;
    }

    /**
     * Driver with example data.<br>
     * 测试入口
     *
     * @param args
     */
    public static void main(String[] args) {

        // words in documents
        int[][] documents = {
                {1, 4, 3, 1, 0},
                {2, 6, 1, 5},
                {3, 1, 0, 1, 4, 6}
        };// 文档的词语id集合

        // vocabulary
        int V = 7;                                      // 词表大小
        int M = documents.length;
        // # topics
        int K = 2;                                      // 主题数目
        // good values alpha = 2, beta = .5
        double alpha = 2;
        double beta = .5;

        System.out.println("Latent Dirichlet Allocation using Gibbs Sampling.");

        GibbsSamplerApp lda = new GibbsSamplerApp(documents, V);
        lda.configure(1000, 200, 100);
        lda.gibbs(K, alpha, beta);

        double[][] theta = lda.getTheta();
        double[][] phi = lda.getPhi(); // inference new document
        lda.printTwoDimArray(lda.z);
        System.out.println("=========================");
        lda.printTwoDimArray(theta);
        System.out.println("=========================");
        lda.printTwoDimArray(phi);
        System.out.println("=========================");

        // Let's inference a new document
        int[] aNewDocument = {2, 2, 4, 2, 4, 2, 2, 2, 2, 4, 2, 2};
        double[] newTheta = inference(alpha, beta, phi, aNewDocument);
        lda.printOneDimArray(newTheta);

        System.out.println();
    }

    public void printTwoDimArray(int[][] arr) {
        int M = arr.length;
        for (int i = 0; i < M; i++) {
            System.out.print("[ ");
            for (int j = 0; j < arr[i].length; j++) {
                System.out.print(arr[i][j] + " ");
            }
            System.out.println("]");
        }
    }

    public void printTwoDimArray(double[][] arr) {
        int M = arr.length;
        for (int i = 0; i < M; i++) {
            System.out.print("[ ");
            for (int j = 0; j < arr[i].length; j++) {
                System.out.print(arr[i][j] + " ");
            }
            System.out.println("]");
        }
    }

    public void printOneDimArray(int[] arr) {
        System.out.print("[ ");
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + " ");
        }
        System.out.println("]");
    }

    public void printOneDimArray(double[] arr) {
        System.out.print("[ ");
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + " ");
        }
        System.out.println("]");
    }
}