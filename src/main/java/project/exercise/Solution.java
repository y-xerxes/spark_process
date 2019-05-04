package project.exercise;

import java.util.*;

class Solution {
    public boolean searchMatrix(int[][] matrix, int target) {
        if (matrix.length==0) {
            return false;
        }
        int x = matrix.length - 1;
        int y = matrix[0].length - 1;
        int a = 0;
        int b = y;
        while (a<x && b>0) {
            if (matrix[a][b] == target) {
                return true;
            } else if (matrix[a][b] < target) {
                a += 1;
            } else {
                b -= 1;
            }
        }
        return false;
    }
}