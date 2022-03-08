import numpy as np
from numpy import array, random

# encoder representations of four different words
"""
Hence, let’s start by first defining the word embeddings of the four different words for which we will 
be calculating the attention. In actual practice, these word embeddings would have been generated by
an encoder, however for this particular example we shall be defining them manually.
"""
word_1 = array([1, 0, 0])
word_2 = array([0, 1, 0])
word_3 = array([1, 1, 0])
word_4 = array([0, 0, 1])

"""
The next step generates the weight matrices, which we will eventually be multiplying to 
the word embeddings to generate the queries, keys and values. Here, we shall be generating 
these weight matrices randomly, however in actual practice these would have been learned during training.
"""

# generating the weight matrices
random.seed(42) # to allow us to reproduce the same attention values
W_Q = random.randint(3, size=(3, 3))
W_K = random.randint(3, size=(3, 3))
W_V = random.randint(3, size=(3, 3))

"""
Notice how the number of rows of each of these matrices is equal to the dimensionality of 
the word embeddings (which in this case is three) to allow us to perform the matrix multiplication.

Subsequently, the query, key and value vectors for each word are generated by multiplying each 
word embedding by each of the weight matrices.    
"""

...
# generating the queries, keys and values
query_1 = word_1 @ W_Q
key_1 = word_1 @ W_K
value_1 = word_1 @ W_V

query_2 = word_2 @ W_Q
key_2 = word_2 @ W_K
value_2 = word_2 @ W_V

query_3 = word_3 @ W_Q
key_3 = word_3 @ W_K
value_3 = word_3 @ W_V

query_4 = word_4 @ W_Q
key_4 = word_4 @ W_K
value_4 = word_4 @ W_V


"""
Considering only the first word for the time being, the next
step scores its query vector against all of the key vectors using a dot product operation.
"""

from numpy import dot
# scoring the first query vector against all key vectors
scores = array([dot(query_1, key_1), dot(query_1, key_2), dot(query_1, key_3), dot(query_1, key_4)])

"""
The score values are subsequently passed through a softmax operation to generate the weights. 
Before doing so, it is common practice to divide the score values by the square
root of the dimensionality of the key vectors (in this case, three), to keep the gradients stable.
"""

from scipy.special import softmax
# computing the weights by a softmax operation
weights = softmax(scores / key_1.shape[0] ** 0.5)

"""
Finally, the attention output is calculated by a weighted sum of all four value vectors. 
"""
# computing the attention by a weighted sum of the value vectors
attention = (weights[0] * value_1) + (weights[1] * value_2) + (weights[2] * value_3) + (weights[3] * value_4)
print(attention)

"""
For faster processing, the same calculations can be implemented in matrix form to generate 
an attention output for all four words in one go:

All in one go
"""

from numpy import array
from numpy import random
from numpy import dot
from scipy.special import softmax

# encoder representations of four different words
word_1 = array([1, 0, 0])
word_2 = array([0, 1, 0])
word_3 = array([1, 1, 0])
word_4 = array([0, 0, 1])

# stacking the word embeddings into a single array
words = array([word_1, word_2, word_3, word_4])

# generating the weight matrices
random.seed(42)
W_Q = random.randint(3, size=(3, 3))
W_K = random.randint(3, size=(3, 3))
W_V = random.randint(3, size=(3, 3))

# generating the queries, keys and values
Q = words @ W_Q
K = words @ W_K
V = words @ W_V

# scoring the query vectors against all key vectors
scores = Q @ K.transpose()

# computing the weights by a softmax operation
weights = softmax(scores / K.shape[1] ** 0.5, axis=1)

# computing the attention by a weighted sum of the value vectors
attention = weights @ V

print(attention)