class Graph:

    def __init__(self, graph_dict=None):
        if graph_dict is None:
            graph_dict = {}
        self.graph_dict = graph_dict

    def get_vertices(self) -> list:
        return list(self.graph_dict.keys())

    # List the edge names (connection between vertices)
    def get_edges(self) -> list:
        edges = []
        for vertex in self.graph_dict:
            for next_vertex in self.graph_dict[vertex]:
                if [next_vertex, vertex] not in edges:
                    edges.append([vertex, next_vertex])
        return edges

    # Add the vertex as a key
    def add_vertex(self, *, vertex) -> None:
        if vertex not in self.graph_dict:
            self.graph_dict[vertex] = []

    # Add the new edge
    def add_edge(self, *, edge) -> None:
        vertex_1, vertex_2 = list(edge)
        if vertex_1 in self.graph_dict:
            self.graph_dict[vertex_1].append(vertex_2)
        else:
            self.graph_dict[vertex_1] = [vertex_2]

    def dfs(self, *, visited, node) -> None:
        if not self.graph_dict:
            return self.graph_dict

        if node not in visited:
            print(node, end=' ')
            visited.append(node)
            for next_vertex in self.graph_dict[node]:
                self.dfs(visited=visited, node=next_vertex)

    def bfs(self, *, visited, queue, node) -> None:
        if not self.graph_dict:
            return self.graph_dict

        queue.append(node)
        visited.append(node)

        while queue:
            s = queue.pop(0)
            print(s, end=' ')
            for vertex in self.graph_dict[s]:
                if vertex not in visited:
                    queue.append(vertex)
                    visited.append(vertex)

