import random
import sys

def generate_random_words(words_number, words_per_line):
    with open("files/random_words.txt", "w") as file:
        for _ in range(words_number):
            word = ''.join(random.choices('abcde', k=random.randint(3, 6)))
            file.write(word)
            if (_ + 1) % words_per_line == 0:
                file.write("\n")
            else:
                file.write(" ")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py words_number words_per_line")
    else:
        words_number = int(sys.argv[1])
        words_per_line = int(sys.argv[2])
        generate_random_words(words_number, words_per_line)
        print(f"File 'random_words.txt' created with {words_number} words, {words_per_line} words per line.")
