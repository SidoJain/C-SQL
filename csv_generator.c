#include <stdio.h>
#include <stdlib.h>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <output_file.csv> <row_count>\n", argv[0]);
        return 1;
    }

    const char* filename = argv[1];
    int row_count = atoi(argv[2]);

    FILE* file = fopen(filename, "w");
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    for (int i = 1; i <= row_count; i++)
        fprintf(file, "%d,user%d,user%d@test.com\n", i, i, i);

    fclose(file);
    printf("Successfully wrote %d rows to '%s'\n", row_count, filename);
    return 0;
}